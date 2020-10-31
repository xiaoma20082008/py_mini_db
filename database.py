from enum import Enum
from typing import Optional, Generic


class SortOrder(Enum):
    DESC = 1,
    ASC = 2

    def __str__(self):
        if self is SortOrder.ASC:
            return 'ASC'
        else:
            return 'DESC'


class DataType(Enum):
    INT = 1,
    CHAR = 2,
    VARCHAR = 3,
    DATETIME = 4,

    def __str__(self):
        if self is DataType.INT:
            return 'INT'
        elif self is DataType.CHAR:
            return 'CHAR'
        elif self is DataType.VARCHAR:
            return 'VARCHAR'
        else:
            return 'DATETIME'

    @staticmethod
    def from_string(data_type: str):
        if data_type == 'int':
            return DataType.INT
        elif data_type == 'char':
            return DataType.CHAR
        elif data_type == 'varchar':
            return DataType.VARCHAR
        elif data_type == 'datetime':
            return DataType.DATETIME
        else:
            raise Exception('unknown type:' + data_type)


class CellMetadata:
    def __init__(self, name, data_type: DataType, value_size: int = 8, is_null: bool = False):
        self.name = name
        self.type = data_type
        self.size = value_size
        self.null = not is_null

    def __str__(self):
        return """%s %s(%d) """ % (self.name, self.type, self.size) + (' NOT NULL' if self.null else ' NULL')


class Cell:
    def __init__(self, cell_value):
        self.value = cell_value

    def __str__(self):
        return """%s""" % (str(self.value))


class Row:
    def __init__(self, metas, cells):
        self.metas = metas
        self.cells = cells

    def __str__(self):
        out = ''
        for i, cell in enumerate(self.cells):
            out += str(cell.value)
            out += '\t\t'
        return out


class RowSet:
    def __init__(self, rows):
        self.rows = rows


class Operator:
    def open(self):
        pass

    def get_next(self) -> Optional[Row]:
        pass

    def close(self):
        pass


class Project(Operator):
    def __init__(self, downstream, columns):
        self.downstream = downstream
        self.columns = columns

    def open(self):
        self.downstream.open()

    def close(self):
        self.downstream.close()

    def get_next(self) -> Optional[Row]:
        row = self.downstream.get_next()
        if not row:
            return None
        else:
            mds = {}
            for i, v in enumerate(row.metas):
                cmd = row.metas[i]
                mds[cmd.name] = (i, cmd, row.cells[i])
            metas = []
            cells = []
            for i, v in enumerate(self.columns):
                out = mds[self.columns[i]]
                metas.append(out[1])
                cells.append(out[2])
            return Row(metas, cells)

    def __str__(self):
        return """ProjectOp(%s)""" % ','.join(self.columns)


class Filter(Operator):
    def __init__(self, downstream: Operator, name, value):
        self.downstream = downstream
        self.name = name
        self.value = value

    def open(self):
        self.downstream.open()

    def get_next(self) -> Optional[Row]:
        while True:
            row = self.downstream.get_next()
            if row is None:
                break
            if self.__filter__(row):
                return row
            else:
                continue
        return None

    def close(self):
        self.downstream.close()

    def __str__(self):
        return """FilterOp(%s=%s)""" % (str(self.name), str(self.value))

    def __filter__(self, row: Row):
        index = -1
        for idx, v in enumerate(row.metas):
            cmd = row.metas[idx]
            if cmd.name == self.name:
                index = idx
                break
            else:
                continue
        if index == -1:
            raise Exception('no column named:' + self.name + ' found')
        cell = row.cells[index]
        if cell.value == self.value:
            return True
        return False


class TableScan(Operator):
    def __init__(self, table, columns, condition=None):
        if condition is None:
            condition = []
        self.table = table
        self.columns = columns
        self.condition = condition
        self.file = None
        self.metadata = {}

    def open(self):
        self.file = open(self.table + '.txt')
        md = self.file.readline()
        # 第一行是空格分割的字符串
        # id:str name:int age:
        for i, col_def_str in enumerate(md.split()):
            col_def = col_def_str.split(':')
            # id:type:size
            cmd = CellMetadata(col_def[0], DataType.from_string(col_def[1]), int(col_def[2]))
            self.metadata[i] = cmd

    def get_next(self) -> Optional[Row]:
        line = self.file.readline()
        if line is None or line == '':
            return None
        cells = []
        lines = line.split()
        metadata = {}
        for i, c in enumerate(lines):
            md = self.metadata[i]
            cell_value = self._convert(c, md)
            cell = Cell(cell_value)
            cells.append(cell)
            metadata[i] = md
        return Row(metadata, cells)

    def close(self):
        self.file.close()

    def __str__(self):
        return """TableScanOp(%s,[%s],[%s])""" % (self.table, ",".join(self.columns), ','.join(self.condition))

    @staticmethod
    def _convert(src, cmd) -> object:
        if cmd.type is DataType.INT:
            return int(src)
        elif cmd.type is DataType.CHAR or cmd.type is DataType.VARCHAR:
            return str(src)
        elif cmd.type is DataType.DATETIME:
            return str(src)
        else:
            raise Exception('unknown type:' + str(cmd))


class ScalarAgg(Operator):
    def __init__(self):
        pass


class Limit(Operator):
    def __init__(self, downstream: Operator, offset: int, limit: int):
        self.downstream = downstream
        self.offset = offset
        self.limit = limit
        self.index = 0

    def open(self):
        self.downstream.open()

    def close(self):
        self.downstream.close()

    def get_next(self) -> Optional[Row]:
        row = self.downstream.get_next()
        if row is not None and self.index < self.limit:
            self.index = self.index + 1
            return row
        else:
            return None

    def __str__(self):
        return """LimitOp(%d,%d)""" % (self.offset, self.limit)


class Sort(Operator):
    def __init__(self, downstream, sort_name, sort_order=SortOrder.ASC):
        self.downstream = downstream
        self.name = sort_name
        self.order = sort_order
        self.index = 0
        self.extra = []

    def open(self):
        self.downstream.open()

        while True:
            row = self.downstream.get_next()
            if row is None:
                break
            else:
                self.extra.append(row)

        merge_sort(self.extra, [(self.name, self.order)])

    def close(self):
        self.index = 0
        self.extra.clear()
        self.downstream.close()

    def get_next(self) -> Optional[Row]:
        if self.index < len(self.extra):
            row = self.extra[self.index]
            self.index = self.index + 1
            return row
        return None

    def __str__(self):
        return """SortOp(%s %s)""" % (self.name, self.order)

    def __compare__(self, lhs: Row, rhs: Row) -> int:
        self.name = ''
        return 0


def merge_sort(rows, sorts):
    pass


class UnaryOp(Operator):
    def __init__(self, downstream: Operator):
        pass


class BinaryOp(Operator):
    def __init__(self, lhs, opt, rhs):
        self.lhs = lhs
        self.opt = opt
        self.rhs = rhs

    def __str__(self):
        return """%s(%s,%s)""" % (str(self.opt), BinaryOp.__filter_to_str(self.lhs), BinaryOp.__filter_to_str(self.rhs))

    @staticmethod
    def __filter_to_str(op: Filter) -> str:
        return """%s=%s""" % (str(op.name), str(op.value))


class DatabaseEngine:
    def __init__(self):
        pass

    def table_scan(self):
        pass


if __name__ == '__main__':
    # id,name,age,addr,email,mobile_no,update_time,create_time
    sql = 'select id,name,age,email,mobile_no from tb1 where id = 10 and name="tom" order by update_time desc limit 5'
    op = TableScan('tb1', [])
    op = Filter(op, 'age', 18)
    op = Filter(op, 'addr', 'sh')
    op = Limit(op, 0, 5)
    op = Project(op, ['name', 'addr', 'mobile_no'])
    op.open()
    while True:
        r = op.get_next()
        if r is None:
            break
        else:
            print(r)
    op.close()
    #
    # Project(columns=[id,name,age,email,mobile_no])
    #   Limit(offset=0, limit=5)
    #     Sort(offset=0, limit=5)
    #       Filter(condition=[id=10])
    #         Filter(condition=[name=tom])
    #           TableScan(table=tb1)
    #
    #           TableScan(table=tb1,columns=[id,name,age,email,mobile_no],condition=[id = 10,name = tom])

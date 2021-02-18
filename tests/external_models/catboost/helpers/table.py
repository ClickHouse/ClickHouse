from server import ClickHouseServer
from client import ClickHouseClient
from pandas import DataFrame
import os
import threading
import tempfile


class ClickHouseTable:
    def __init__(self, server, port, table_name, df):
        self.server = server
        self.port = port
        self.table_name = table_name
        self.df = df

        if not isinstance(self.server, ClickHouseServer):
            raise Exception('Expected ClickHouseServer, got ' + repr(self.server))
        if not isinstance(self.df, DataFrame):
            raise Exception('Expected DataFrame, got ' + repr(self.df))

        self.server.wait_for_request(port)
        self.client = ClickHouseClient(server.binary_path, port)

    def _convert(self, name):
        types_map = {
            'float64': 'Float64',
            'int64': 'Int64',
            'float32': 'Float32',
            'int32': 'Int32'
        }

        if name in types_map:
            return types_map[name]
        return 'String'

    def _create_table_from_df(self):
        self.client.query('create database if not exists test')
        self.client.query('drop table if exists test.{}'.format(self.table_name))

        column_types = list(self.df.dtypes)
        column_names = list(self.df)
        schema = ', '.join((name + ' ' + self._convert(str(t)) for name, t in zip(column_names, column_types)))
        print 'schema:', schema

        create_query = 'create table test.{} (date Date DEFAULT today(), {}) engine = MergeTree(date, (date), 8192)'
        self.client.query(create_query.format(self.table_name, schema))

        insert_query = 'insert into test.{} ({}) format CSV'

        with tempfile.TemporaryFile() as tmp_file:
            self.df.to_csv(tmp_file, header=False, index=False)
            tmp_file.seek(0)
            self.client.query(insert_query.format(self.table_name, ', '.join(column_names)), pipe=tmp_file)

    def apply_model(self, model_name, float_columns, cat_columns):
        columns = ', '.join(list(float_columns) + list(cat_columns))
        query = "select modelEvaluate('{}', {}) from test.{} format TSV"
        result = self.client.query(query.format(model_name, columns, self.table_name))

        def parse_row(row):
            values = tuple(map(float, filter(len, map(str.strip, row.replace('(', '').replace(')', '').split(',')))))
            return values if len(values) != 1 else values[0]

        return tuple(map(parse_row, filter(len, map(str.strip, result.split('\n')))))

    def _drop_table(self):
        self.client.query('drop table test.{}'.format(self.table_name))

    def __enter__(self):
        self._create_table_from_df()
        return self

    def __exit__(self, type, value, traceback):
        self._drop_table()

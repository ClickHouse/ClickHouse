# -*- coding: utf-8 -*-
import warnings
import pymysql.cursors
import pymongo
import subprocess
import copy

class ExternalSource(object):
    def __init__(self, name, internal_hostname, internal_port,
                 docker_hostname, docker_port, user, password):
        self.name = name
        self.internal_hostname = internal_hostname
        self.internal_port = int(internal_port)
        self.docker_hostname = docker_hostname
        self.docker_port = int(docker_port)
        self.user = user
        self.password = password

    def get_source_str(self, table_name):
        raise NotImplementedError("Method {} is not implemented for {}".format(
            "get_source_config_part", self.__class__.__name__))

    def prepare(self, structure, table_name, cluster):
        raise NotImplementedError("Method {} is not implemented for {}".format(
            "prepare_remote_source", self.__class__.__name__))

    # data is banch of Row
    def load_data(self, data):
        raise NotImplementedError("Method {} is not implemented for {}".format(
            "prepare_remote_source", self.__class__.__name__))

    def compatible_with_layout(self, layout):
        return True


class SourceMySQL(ExternalSource):
    TYPE_MAPPING = {
        'UInt8': 'tinyint unsigned',
        'UInt16': 'smallint unsigned',
        'UInt32': 'int unsigned',
        'UInt64': 'bigint unsigned',
        'Int8': 'tinyint',
        'Int16': 'smallint',
        'Int32': 'int',
        'Int64': 'bigint',
        'UUID': 'varchar(36)',
        'Date': 'date',
        'DateTime': 'datetime',
        'String': 'text',
        'Float32': 'float',
        'Float64': 'double'
    }
    def create_mysql_conn(self):
        self.connection = pymysql.connect(
            user=self.user,
            password=self.password,
            host=self.internal_hostname,
            port=self.internal_port)

    def execute_mysql_query(self, query):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.connection.cursor() as cursor:
                cursor.execute(query)
            self.connection.commit()

    def get_source_str(self, table_name):
        return '''
                <mysql>
                    <replica>
                        <priority>1</priority>
                        <host>127.0.0.1</host>
                        <port>3333</port>   <!-- Wrong port, for testing basic failover to work. -->
                    </replica>
                    <replica>
                        <priority>2</priority>
                        <host>{hostname}</host>
                        <port>{port}</port>
                    </replica>
                    <user>{user}</user>
                    <password>{password}</password>
                    <db>test</db>
                    <table>{tbl}</table>
                </mysql>'''.format(
                hostname=self.docker_hostname,
                port=self.docker_port,
                user=self.user,
                password=self.password,
                tbl=table_name,
            )

    def prepare(self, structure, table_name, cluster):
        self.create_mysql_conn()
        self.execute_mysql_query("create database if not exists test default character set 'utf8'")
        fields_strs = []
        for field in structure.keys + structure.ordinary_fields + structure.range_fields:
            fields_strs.append(field.name + ' ' + self.TYPE_MAPPING[field.field_type])
        create_query = '''create table test.{table_name} (
            {fields_str});
        '''.format(table_name=table_name, fields_str=','.join(fields_strs))
        self.execute_mysql_query(create_query)
        self.ordered_names = structure.get_ordered_names()
        self.prepared = True

    def load_data(self, data, table_name):
        values_strs = []
        if not data:
            return
        for row in data:
            sorted_row = []
            for name in self.ordered_names:
                data = row.data[name]
                if isinstance(row.data[name], str):
                    data = "'" + data + "'"
                else:
                    data = str(data)
                sorted_row.append(data)
            values_strs.append('(' + ','.join(sorted_row) + ')')
        query = 'insert into test.{} ({}) values {}'.format(
            table_name,
            ','.join(self.ordered_names),
            ''.join(values_strs))
        self.execute_mysql_query(query)


class SourceMongo(ExternalSource):

    def get_source_str(self, table_name):
        return '''
            <mongodb>
                <host>{host}</host>
                <port>{port}</port>
                <user>{user}</user>
                <password>{password}</password>
                <db>test</db>
                <collection>{tbl}</collection>
            </mongodb>
        '''.format(
            host=self.docker_hostname,
            port=self.docker_port,
            user=self.user,
            password=self.password,
            tbl=table_name,
        )

    def prepare(self, structure, table_name, cluster):
        connection_str = 'mongodb://{user}:{password}@{host}:{port}'.format(
            host=self.internal_hostname, port=self.internal_port,
            user=self.user, password=self.password)
        self.connection = pymongo.MongoClient(connection_str)
        self.connection.create
        self.structure = structure
        self.db = self.connection['test']
        self.prepared = True

    def load_data(self, data, table_name):
        tbl = self.db[table_name]
        to_insert = [dict(row.data) for row in data]
        result = tbl.insert_many(to_insert)

class SourceClickHouse(ExternalSource):

    def get_source_str(self, table_name):
        return '''
            <clickhouse>
                <host>{host}</host>
                <port>{port}</port>
                <user>{user}</user>
                <password>{password}</password>
                <db>test</db>
                <table>{tbl}</table>
            </clickhouse>
        '''.format(
            host=self.docker_hostname,
            port=self.docker_port,
            user=self.user,
            password=self.password,
            tbl=table_name,
        )

    def prepare(self, structure, table_name, cluster):
        self.node = cluster.instances[self.docker_hostname]
        self.node.query("CREATE DATABASE IF NOT EXISTS test")
        fields_strs = []
        for field in structure.keys + structure.ordinary_fields + structure.range_fields:
            fields_strs.append(field.name + ' ' + field.field_type)
        create_query = '''CREATE TABLE test.{table_name} (
            {fields_str}) ENGINE MergeTree ORDER BY tuple();
        '''.format(table_name=table_name, fields_str=','.join(fields_strs))
        self.node.query(create_query)
        self.ordered_names = structure.get_ordered_names()
        self.prepared = True

    def load_data(self, data, table_name):
        values_strs = []
        if not data:
            return
        for row in data:
            sorted_row = []
            for name in self.ordered_names:
                row_data = row.data[name]
                if isinstance(row_data, str):
                    row_data = "'" + row_data + "'"
                else:
                    row_data = str(row_data)
                sorted_row.append(row_data)
            values_strs.append('(' + ','.join(sorted_row) + ')')
        query = 'INSERT INTO test.{} ({}) values {}'.format(
            table_name,
            ','.join(self.ordered_names),
            ''.join(values_strs))
        self.node.query(query)


class SourceFile(ExternalSource):

    def get_source_str(self, table_name):
        table_path = "/" + table_name + ".tsv"
        return '''
            <file>
                <path>{path}</path>
                <format>TabSeparated</format>
            </file>
        '''.format(
            path=table_path,
        )

    def prepare(self, structure, table_name, cluster):
        self.node = cluster.instances[self.docker_hostname]
        path = "/" + table_name + ".tsv"
        self.node.exec_in_container(["bash", "-c", "touch {}".format(path)])
        self.ordered_names = structure.get_ordered_names()
        self.prepared = True

    def load_data(self, data, table_name):
        if not data:
            return
        path = "/" + table_name + ".tsv"
        for row in list(data):
            sorted_row = []
            for name in self.ordered_names:
                sorted_row.append(str(row.data[name]))

            str_data = '\t'.join(sorted_row)
            self.node.exec_in_container(["bash", "-c", "echo \"{row}\" >> {fname}".format(row=str_data, fname=path)])

    def compatible_with_layout(self, layout):
        return 'cache' not in layout.name


class _SourceExecutableBase(ExternalSource):

    def _get_cmd(self, path):
        raise NotImplementedError("Method {} is not implemented for {}".format(
            "_get_cmd", self.__class__.__name__))

    def get_source_str(self, table_name):
        table_path = "/" + table_name + ".tsv"
        return '''
            <executable>
                <command>{cmd}</command>
                <format>TabSeparated</format>
            </executable>
        '''.format(
            cmd=self._get_cmd(table_path),
        )

    def prepare(self, structure, table_name, cluster):
        self.node = cluster.instances[self.docker_hostname]
        path = "/" + table_name + ".tsv"
        self.node.exec_in_container(["bash", "-c", "touch {}".format(path)])
        self.ordered_names = structure.get_ordered_names()
        self.prepared = True

    def load_data(self, data, table_name):
        if not data:
            return
        path = "/" + table_name + ".tsv"
        for row in list(data):
            sorted_row = []
            for name in self.ordered_names:
                sorted_row.append(str(row.data[name]))

            str_data = '\t'.join(sorted_row)
            self.node.exec_in_container(["bash", "-c", "echo \"{row}\" >> {fname}".format(row=str_data, fname=path)])


class SourceExecutableCache(_SourceExecutableBase):

    def _get_cmd(self, path):
        return "cat {}".format(path)

    def compatible_with_layout(self, layout):
        return 'cache' not in layout.name


class SourceExecutableHashed(_SourceExecutableBase):

    def _get_cmd(self, path):
        return "cat - >/dev/null;cat {}".format(path)

    def compatible_with_layout(self, layout):
        return 'cache' in layout.name

class SourceHTTPBase(ExternalSource):

    def get_source_str(self, table_name):
        url = "{schema}://{host}:5555/".format(schema=self._get_schema(), self.docker_hostname)
        return '''
            <http>
                <url>{url}</url>
                <format>TabSeparated</format>
            </http>
        '''.format(url=url)

    def prepare(self, structure, table_name, cluster):
        self.node = cluster.instances[self.docker_hostname]
        path = "/" + table_name + ".tsv"
        self.node.exec_in_container(["bash", "-c", "touch {}".format(path)])
        with open('http_server.py', 'r') as server_code:
            for line in server_code:
                self.node.exec_in_container(["bash", "-c", "echo ".format(path)])

        self.node.exec_in_container([])
        self.ordered_names = structure.get_ordered_names()
        self.prepared = True

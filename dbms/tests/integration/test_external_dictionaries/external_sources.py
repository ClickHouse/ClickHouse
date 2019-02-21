# -*- coding: utf-8 -*-
import warnings
import pymysql.cursors
import pymongo

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

    def get_source_str(self):
        raise NotImplementedError("Method {} is not implemented for {}".format(
            "get_source_config_part", self.__class__.__name__))

    def prepare(self, structure):
        raise NotImplementedError("Method {} is not implemented for {}".format(
            "prepare_remote_source", self.__class__.__name__))

    # data is banch of Row
    def load_data(self, data):
        raise NotImplementedError("Method {} is not implemented for {}".format(
            "prepare_remote_source", self.__class__.__name__))

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

    def prepare(self, structure, table_name):
        self.create_mysql_conn()
        self.execute_mysql_query("create database if not exists test default character set 'utf8'")
        fields_strs = []
        for field in structure.keys + structure.ordinary_fields + structure.range_fields:
            fields_strs.append(field.name + ' ' + self.TYPE_MAPPING[field.field_type])
        create_query = '''create table test.{table_name} (
            {fields_str});
        '''.format(table_name=table_name, fields_str=','.join(fields_strs))
        self.execute_mysql_query(create_query)
        self.prepared = True

    def load_data(self, data, table_name):
        values_strs = []
        if not data:
            return
        ordered_names = [name for name in data[0].data]
        for row in data:
            sorted_row = []
            for name in ordered_names:
                data = row.data[name]
                if isinstance(row.data[name], str):
                    data = "'" + data + "'"
                else:
                    data = str(data)
                sorted_row.append(data)
            values_strs.append('(' + ','.join(sorted_row) + ')')
        query = 'insert into test.{} ({}) values {}'.format(
            table_name,
            ','.join(ordered_names),
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

    def prepare(self, structure, table_name):
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
        print "IDS:", result.inserted_ids
        for r in tbl.find():
            print "RESULT:", r

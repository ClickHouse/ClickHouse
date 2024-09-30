import time

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

postgres_table_template = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key Integer NOT NULL, value Integer, PRIMARY KEY(key))
    """
postgres_table_template_2 = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key Integer NOT NULL, value1 Integer, value2 Integer, value3 Integer, PRIMARY KEY(key))
    """
postgres_table_template_3 = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key1 Integer NOT NULL, value1 Integer, key2 Integer NOT NULL, value2 Integer NOT NULL)
    """
postgres_table_template_4 = """
    CREATE TABLE IF NOT EXISTS "{}"."{}" (
    key Integer NOT NULL, value Integer, PRIMARY KEY(key))
    """
postgres_table_template_5 = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key Integer NOT NULL, value UUID, PRIMARY KEY(key))
    """
postgres_table_template_6 = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key Integer NOT NULL, value Text, PRIMARY KEY(key))
    """


def get_postgres_conn(
    ip,
    port,
    database=False,
    auto_commit=True,
    database_name="postgres_database",
    replication=False,
):
    if database == True:
        conn_string = f"host={ip} port={port} dbname='{database_name}' user='postgres' password='mysecretpassword'"
    else:
        conn_string = (
            f"host={ip} port={port} user='postgres' password='mysecretpassword'"
        )

    if replication:
        conn_string += " replication='database'"

    conn = psycopg2.connect(conn_string)
    if auto_commit:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        conn.autocommit = True
    return conn


def create_replication_slot(conn, slot_name="user_slot"):
    cursor = conn.cursor()
    cursor.execute(
        f"CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput EXPORT_SNAPSHOT"
    )
    result = cursor.fetchall()
    print(result[0][0])  # slot name
    print(result[0][1])  # start lsn
    print(result[0][2])  # snapshot
    return result[0][2]


def drop_replication_slot(conn, slot_name="user_slot"):
    cursor = conn.cursor()
    cursor.execute(f"select pg_drop_replication_slot('{slot_name}')")


def create_postgres_schema(cursor, schema_name):
    drop_postgres_schema(cursor, schema_name)
    cursor.execute(f"CREATE SCHEMA {schema_name}")


def drop_postgres_schema(cursor, schema_name):
    cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def create_postgres_table(
    cursor,
    table_name,
    replica_identity_full=False,
    template=postgres_table_template,
):
    drop_postgres_table(cursor, table_name)
    query = template.format(table_name)

    print(f"Query: {query}")
    cursor.execute(query)

    if replica_identity_full:
        cursor.execute(f"""ALTER TABLE "{table_name}" REPLICA IDENTITY FULL;""")


def drop_postgres_table(cursor, name, database_name=""):
    if database_name != "":
        cursor.execute(f"""DROP TABLE IF EXISTS "{database_name}"."{name}" """)
    else:
        cursor.execute(f"""DROP TABLE IF EXISTS "{name}" """)


def create_postgres_table_with_schema(cursor, schema_name, table_name):
    drop_postgres_table_with_schema(cursor, schema_name, table_name)
    cursor.execute(postgres_table_template_4.format(schema_name, table_name))


def drop_postgres_table_with_schema(cursor, schema_name, table_name):
    cursor.execute(f"""DROP TABLE IF EXISTS "{schema_name}"."{table_name}" """)


class PostgresManager:
    def __init__(self):
        self.created_postgres_db_list = set()
        self.created_materialized_postgres_db_list = set()
        self.created_ch_postgres_db_list = set()

    def init(
        self,
        instance,
        ip,
        port,
        default_database="postgres_database",
        postgres_db_exists=False,
    ):
        self.instance = instance
        self.ip = ip
        self.port = port
        self.default_database = default_database
        self.postgres_db_exists = postgres_db_exists
        self.prepare()

    def get_default_database(self):
        return self.default_database

    def restart(self):
        try:
            self.clear()
            self.prepare()
        except Exception as ex:
            self.prepare()
            raise ex

    def execute(self, query):
        return self.cursor.execute(query)

    def prepare(self):
        self.conn = get_postgres_conn(ip=self.ip, port=self.port)
        self.cursor = self.conn.cursor()
        if self.default_database != "":
            if not self.postgres_db_exists:
                self.create_postgres_db(self.default_database)
            self.conn = get_postgres_conn(
                ip=self.ip,
                port=self.port,
                database=True,
                database_name=self.default_database,
            )
            self.cursor = self.conn.cursor()
            self.create_clickhouse_postgres_db()

    def clear(self):
        if self.conn.closed == 0:
            self.conn.close()
        for db in self.created_materialized_postgres_db_list.copy():
            self.drop_materialized_db(db)
        for db in self.created_ch_postgres_db_list.copy():
            self.drop_clickhouse_postgres_db(db)
        if len(self.created_postgres_db_list) > 0:
            self.conn = get_postgres_conn(ip=self.ip, port=self.port)
            self.cursor = self.conn.cursor()
            for db in self.created_postgres_db_list.copy():
                self.drop_postgres_db(db)

    def get_db_cursor(self, database_name=""):
        if database_name == "":
            database_name = self.default_database
        self.conn = get_postgres_conn(
            ip=self.ip, port=self.port, database=True, database_name=database_name
        )
        return self.conn.cursor()

    def database_or_default(self, database_name):
        if database_name != "":
            return database_name
        if self.default_database != "":
            return self.default_database
        raise Exception("Database name is empty")

    def create_postgres_db(self, database_name=""):
        database_name = self.database_or_default(database_name)
        self.drop_postgres_db(database_name)
        self.created_postgres_db_list.add(database_name)
        self.cursor.execute(f'CREATE DATABASE "{database_name}"')

    def drop_postgres_db(self, database_name=""):
        database_name = self.database_or_default(database_name)
        self.cursor.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
        if database_name in self.created_postgres_db_list:
            self.created_postgres_db_list.remove(database_name)

    def create_clickhouse_postgres_db(
        self,
        database_name="",
        schema_name="",
        postgres_database="",
    ):
        database_name = self.database_or_default(database_name)
        if postgres_database == "":
            postgres_database = database_name
        self.drop_clickhouse_postgres_db(database_name)
        self.created_ch_postgres_db_list.add(database_name)

        if len(schema_name) == 0:
            self.instance.query(
                f"""
                    CREATE DATABASE \"{database_name}\"
                    ENGINE = PostgreSQL('{self.ip}:{self.port}', '{postgres_database}', 'postgres', 'mysecretpassword')"""
            )
        else:
            self.instance.query(
                f"""
                CREATE DATABASE \"{database_name}\"
                ENGINE = PostgreSQL('{self.ip}:{self.port}', '{postgres_database}', 'postgres', 'mysecretpassword', '{schema_name}')"""
            )

    def drop_clickhouse_postgres_db(self, database_name=""):
        database_name = self.database_or_default(database_name)
        self.instance.query(f'DROP DATABASE IF EXISTS "{database_name}"')
        if database_name in self.created_ch_postgres_db_list:
            self.created_ch_postgres_db_list.remove(database_name)

    def create_materialized_db(
        self,
        ip,
        port,
        materialized_database="test_database",
        postgres_database="",
        settings=[],
        table_overrides="",
        user="postgres",
        password="mysecretpassword",
    ):
        postgres_database = self.database_or_default(postgres_database)
        self.created_materialized_postgres_db_list.add(materialized_database)
        self.instance.query(f"DROP DATABASE IF EXISTS `{materialized_database}`")

        create_query = f"CREATE DATABASE `{materialized_database}` ENGINE = MaterializedPostgreSQL('{ip}:{port}', '{postgres_database}', '{user}', '{password}')"
        if len(settings) > 0:
            create_query += " SETTINGS "
            for i in range(len(settings)):
                if i != 0:
                    create_query += ", "
                create_query += settings[i]
        create_query += table_overrides
        self.instance.query(create_query)
        assert materialized_database in self.instance.query("SHOW DATABASES")

    def drop_materialized_db(self, materialized_database="test_database"):
        self.instance.query(f"DROP DATABASE IF EXISTS `{materialized_database}` SYNC")
        if materialized_database in self.created_materialized_postgres_db_list:
            self.created_materialized_postgres_db_list.remove(materialized_database)

    def create_postgres_schema(self, name):
        create_postgres_schema(self.cursor, name)

    def create_postgres_table(
        self, table_name, database_name="", template=postgres_table_template
    ):
        database_name = self.database_or_default(database_name)
        cursor = self.cursor
        if database_name != self.get_default_database:
            try:
                self.create_postgres_db(database_name)
            except:
                # postgres does not support create database if not exists
                pass
            conn = get_postgres_conn(
                ip=self.ip,
                port=self.port,
                database=True,
                database_name=database_name,
            )
            cursor = conn.cursor()
        create_postgres_table(cursor, table_name, template=template)

    def create_and_fill_postgres_table(self, table_name, database_name=""):
        database_name = self.database_or_default(database_name)
        self.create_postgres_table(table_name, database_name)
        self.instance.query(
            f"INSERT INTO `{database_name}`.`{table_name}` SELECT number, number from numbers(50)"
        )

    def create_and_fill_postgres_tables(
        self,
        tables_num,
        numbers=50,
        database_name="",
        table_name_base="postgresql_replica",
    ):
        for i in range(tables_num):
            table_name = f"{table_name_base}_{i}"
            self.create_postgres_table(table_name, database_name)
            if numbers > 0:
                db = self.database_or_default(database_name)
                self.instance.query(
                    f"INSERT INTO `{db}`.{table_name} SELECT number, number from numbers({numbers})"
                )


queries = [
    "INSERT INTO postgresql_replica_{} select i, i from generate_series(0, 10000) as t(i);",
    "DELETE FROM postgresql_replica_{} WHERE (value*value) % 3 = 0;",
    "UPDATE postgresql_replica_{} SET value = value - 125 WHERE key % 2 = 0;",
    "UPDATE postgresql_replica_{} SET key=key+20000 WHERE key%2=0",
    "INSERT INTO postgresql_replica_{} select i, i from generate_series(40000, 50000) as t(i);",
    "DELETE FROM postgresql_replica_{} WHERE key % 10 = 0;",
    "UPDATE postgresql_replica_{} SET value = value + 101 WHERE key % 2 = 1;",
    "UPDATE postgresql_replica_{} SET key=key+80000 WHERE key%2=1",
    "DELETE FROM postgresql_replica_{} WHERE value % 2 = 0;",
    "UPDATE postgresql_replica_{} SET value = value + 2000 WHERE key % 5 = 0;",
    "INSERT INTO postgresql_replica_{} select i, i from generate_series(200000, 250000) as t(i);",
    "DELETE FROM postgresql_replica_{} WHERE value % 3 = 0;",
    "UPDATE postgresql_replica_{} SET value = value * 2 WHERE key % 3 = 0;",
    "UPDATE postgresql_replica_{} SET key=key+500000 WHERE key%2=1",
    "INSERT INTO postgresql_replica_{} select i, i from generate_series(1000000, 1050000) as t(i);",
    "DELETE FROM postgresql_replica_{} WHERE value % 9 = 2;",
    "UPDATE postgresql_replica_{} SET key=key+10000000",
    "UPDATE postgresql_replica_{} SET value = value + 2  WHERE key % 3 = 1;",
    "DELETE FROM postgresql_replica_{} WHERE value%5 = 0;",
]


def assert_nested_table_is_created(
    instance, table_name, materialized_database="test_database", schema_name=""
):
    if len(schema_name) == 0:
        table = table_name
    else:
        table = schema_name + "." + table_name

    print(f"Checking table {table} exists in {materialized_database}")
    database_tables = instance.query(
        f"SHOW TABLES FROM `{materialized_database}` WHERE name = '{table}'"
    )

    while table not in database_tables:
        time.sleep(0.2)
        database_tables = instance.query(
            f"SHOW TABLES FROM `{materialized_database}` WHERE name = '{table}'"
        )

    assert table in database_tables


def assert_number_of_columns(
    instance, expected, table_name, database_name="test_database"
):
    result = instance.query(
        f"select count() from system.columns where table = '{table_name}' and database = '{database_name}' and not startsWith(name, '_')"
    )
    while int(result) != expected:
        time.sleep(1)
        result = instance.query(
            f"select count() from system.columns where table = '{table_name}' and database = '{database_name}' and not startsWith(name, '_')"
        )
    print("Number of columns ok")


def check_tables_are_synchronized(
    instance,
    table_name,
    order_by="key",
    postgres_database="postgres_database",
    materialized_database="test_database",
    schema_name="",
    columns=["*"],
):
    assert_nested_table_is_created(
        instance, table_name, materialized_database, schema_name
    )

    table_path = ""
    if len(schema_name) == 0:
        table_path = f"`{materialized_database}`.`{table_name}`"
    else:
        table_path = f"`{materialized_database}`.`{schema_name}.{table_name}`"

    print(f"Checking table is synchronized: {table_path}")
    result_query = f"select * from {table_path} order by {order_by};"

    expected = instance.query(
        f"select {','.join(columns)} from `{postgres_database}`.`{table_name}` order by {order_by};"
    )
    result = instance.query(result_query)

    for _ in range(50):
        if result == expected:
            break
        else:
            time.sleep(1)
        result = instance.query(result_query)

    if result != expected:
        count = int(instance.query(f"select count() from {table_path}"))
        expected_count = int(
            instance.query(f"select count() from `{postgres_database}`.`{table_name}`")
        )
        print(f"Having {count}, expected {expected_count}")
    assert result == expected


def check_several_tables_are_synchronized(
    instance,
    tables_num,
    order_by="key",
    postgres_database="postgres_database",
    materialized_database="test_database",
    schema_name="",
):
    for i in range(tables_num):
        check_tables_are_synchronized(
            instance,
            f"postgresql_replica_{i}",
            postgres_database=postgres_database,
            materialized_database=materialized_database,
        )

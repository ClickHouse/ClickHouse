import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import (
    PostgresManager,
    assert_number_of_columns,
    check_several_tables_are_synchronized,
    check_tables_are_synchronized,
    get_postgres_conn,
    postgres_table_template_6,
)
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/log_conf.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
    stay_alive=True,
)

instance2 = cluster.add_instance(
    "instance2",
    main_configs=["configs/log_conf.xml", "configs/merge_tree_too_many_parts.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
    stay_alive=True,
)


pg_manager = PostgresManager()
pg_manager2 = PostgresManager()
pg_manager_instance2 = PostgresManager()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        pg_manager.init(
            instance,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
        )
        pg_manager_instance2.init(
            instance2,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
            postgres_db_exists=True,
        )
        pg_manager2.init(
            instance2, cluster.postgres_ip, cluster.postgres_port, "postgres_database2"
        )

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    print("PostgreSQL is available - running test")
    yield  # run test
    pg_manager.restart()


def test_table_override(started_cluster):
    table_name = "table_override"
    materialized_database = "test_database"

    pg_manager.create_postgres_table(table_name, template=postgres_table_template_6)
    instance.query(
        f"insert into postgres_database.{table_name} select number, 'test' from numbers(10)"
    )

    table_overrides = f" TABLE OVERRIDE {table_name} (COLUMNS (key Int32, value String) PARTITION BY key)"
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[f"materialized_postgresql_tables_list = '{table_name}'"],
        materialized_database=materialized_database,
        table_overrides=table_overrides,
    )

    check_tables_are_synchronized(
        instance, table_name, postgres_database=pg_manager.get_default_database()
    )

    assert 10 == int(
        instance.query(f"SELECT count() FROM {materialized_database}.{table_name}")
    )

    expected = "CREATE TABLE test_database.table_override\\n(\\n    `key` Int32,\\n    `value` String,\\n    `_sign` Int8 MATERIALIZED 1,\\n    `_version` UInt64 MATERIALIZED 1\\n)\\nENGINE = ReplacingMergeTree(_version)\\nPARTITION BY key\\nORDER BY tuple(key)"
    assert (
        expected
        == instance.query(
            f"show create table {materialized_database}.{table_name}"
        ).strip()
    )

    assert (
        "test"
        == instance.query(
            f"SELECT value FROM {materialized_database}.{table_name} WHERE key = 2"
        ).strip()
    )

    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database_name="postgres_database",
        database=True,
        auto_commit=True,
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT count(*) FROM {table_name}")
    assert 10 == cursor.fetchall()[0][0]

    pg_manager.execute(f"UPDATE {table_name} SET value='kek' WHERE key=2")

    cursor.execute(f"SELECT value FROM {table_name} WHERE key=2")
    assert "kek" == cursor.fetchall()[0][0]

    pg_manager.execute(f"DELETE FROM {table_name} WHERE key=2")

    cursor.execute(f"SELECT count(*) FROM {table_name}")
    assert 9 == cursor.fetchall()[0][0]

    conn.close()

    check_tables_are_synchronized(
        instance, table_name, postgres_database=pg_manager.get_default_database()
    )

    assert (
        ""
        == instance.query(
            f"SELECT value FROM {materialized_database}.{table_name} WHERE key = 2"
        ).strip()
    )


def test_materialized_view(started_cluster):
    pg_manager.execute("DROP TABLE IF EXISTS test_table")
    pg_manager.execute(
        "CREATE TABLE test_table (key integer PRIMARY KEY, value integer)"
    )
    pg_manager.execute("INSERT INTO test_table SELECT 1, 2")
    instance.query("DROP DATABASE IF EXISTS test_database")
    instance.query(
        "CREATE DATABASE test_database ENGINE = MaterializedPostgreSQL(postgres1) SETTINGS materialized_postgresql_tables_list='test_table'"
    )
    check_tables_are_synchronized(instance, "test_table")
    instance.query("DROP TABLE IF EXISTS mv")
    instance.query(
        "CREATE MATERIALIZED VIEW mv ENGINE=MergeTree ORDER BY tuple() POPULATE AS SELECT * FROM test_database.test_table"
    )
    assert "1\t2" == instance.query("SELECT * FROM mv").strip()
    pg_manager.execute("INSERT INTO test_table SELECT 3, 4")
    check_tables_are_synchronized(instance, "test_table")
    assert "1\t2\n3\t4" == instance.query("SELECT * FROM mv ORDER BY 1, 2").strip()
    instance.query("DROP VIEW mv")
    pg_manager.drop_materialized_db()


def test_too_many_parts(started_cluster):
    table = "test_table"
    pg_manager2.create_and_fill_postgres_table(table)
    pg_manager2.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_tables_list = 'test_table', materialized_postgresql_backoff_min_ms = 100, materialized_postgresql_backoff_max_ms = 100"
        ],
    )
    check_tables_are_synchronized(
        instance2, "test_table", postgres_database=pg_manager2.get_default_database()
    )
    assert (
        "50" == instance2.query("SELECT count() FROM test_database.test_table").strip()
    )

    instance2.query("SYSTEM STOP MERGES")
    num = 50
    for i in range(10):
        instance2.query(
            f"""
            INSERT INTO {pg_manager2.get_default_database()}.test_table SELECT {num}, {num};
        """
        )
        num = num + 1
        for i in range(30):
            if num == int(
                instance2.query("SELECT count() FROM test_database.test_table")
            ) or instance2.contains_in_log("DB::Exception: Too many parts"):
                break
            time.sleep(1)
            print(f"wait sync try {i}")
        instance2.query("SYSTEM FLUSH LOGS")
        if instance2.contains_in_log("DB::Exception: Too many parts"):
            break
        assert num == int(
            instance2.query("SELECT count() FROM test_database.test_table")
        ) or num - 1 == int(
            instance2.query("SELECT count() FROM test_database.test_table")
        )

    assert instance2.contains_in_log("DB::Exception: Too many parts")
    print(num)
    assert num == int(
        instance2.query("SELECT count() FROM test_database.test_table")
    ) or num - 1 == int(instance2.query("SELECT count() FROM test_database.test_table"))

    instance2.query("SYSTEM START MERGES")
    check_tables_are_synchronized(
        instance2, "test_table", postgres_database=pg_manager2.get_default_database()
    )

    # assert "200" == instance.query("SELECT count FROM test_database.test_table").strip()
    pg_manager2.drop_materialized_db()


def test_toast(started_cluster):
    table = "test_toast"
    pg_manager.create_postgres_table(
        table,
        "",
        """CREATE TABLE "{}" (id integer PRIMARY KEY, txt text, other text)""",
    )
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_tables_list = '{table}'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )

    pg_manager.execute(
        f"""\
INSERT INTO {table} (id, txt)\
VALUES (1, (SELECT array_to_string(ARRAY(SELECT chr((100 + round(random() * 25)) :: integer) FROM generate_series(1,30000) as t(i)), '')))
    """
    )

    check_tables_are_synchronized(
        instance,
        table,
        postgres_database=pg_manager.get_default_database(),
        order_by="id",
    )


def test_replica_consumer(started_cluster):
    table = "test_replica_consumer"
    pg_manager_instance2.restart()

    pg_manager.create_postgres_table(table)
    instance.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number from numbers(0, 50)"
    )

    for pm in [pg_manager, pg_manager_instance2]:
        pm.create_materialized_db(
            ip=started_cluster.postgres_ip,
            port=started_cluster.postgres_port,
            settings=[
                f"materialized_postgresql_tables_list = '{table}'",
                "materialized_postgresql_backoff_min_ms = 100",
                "materialized_postgresql_backoff_max_ms = 100",
                "materialized_postgresql_use_unique_replication_consumer_identifier = 1",
            ],
        )

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )
    check_tables_are_synchronized(
        instance2, table, postgres_database=pg_manager_instance2.get_default_database()
    )

    assert 50 == int(instance.query(f"SELECT count() FROM test_database.{table}"))
    assert 50 == int(instance2.query(f"SELECT count() FROM test_database.{table}"))

    instance.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number from numbers(1000, 1000)"
    )

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )
    check_tables_are_synchronized(
        instance2, table, postgres_database=pg_manager_instance2.get_default_database()
    )

    assert 1050 == int(instance.query(f"SELECT count() FROM test_database.{table}"))
    assert 1050 == int(instance2.query(f"SELECT count() FROM test_database.{table}"))

    for pm in [pg_manager, pg_manager_instance2]:
        pm.drop_materialized_db()
    pg_manager_instance2.clear()


def test_bad_connection_options(started_cluster):
    table = "test_bad_connection_options"

    pg_manager.create_postgres_table(table)
    instance.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number from numbers(0, 50)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_tables_list = '{table}'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
        user="postrges",
        password="kek",
    )

    instance.wait_for_log_line('role "postrges" does not exist')
    assert instance.contains_in_log(
        "<Error> void DB::DatabaseMaterializedPostgreSQL::startSynchronization(): std::exception. Code: 1001, type: pqxx::broken_connection"
    )
    assert "test_database" in instance.query("SHOW DATABASES")
    assert "" == instance.query("show tables from test_database").strip()
    pg_manager.drop_materialized_db("test_database")


def test_failed_load_from_snapshot(started_cluster):
    if instance.is_built_with_sanitizer() or instance.is_debug_build():
        pytest.skip(
            "Sanitizers and debug mode are skipped, because this test thrown logical error"
        )

    table = "failed_load"

    pg_manager.create_postgres_table(
        table,
        template="""
    CREATE TABLE IF NOT EXISTS "{}" (
    key text NOT NULL, value text[], PRIMARY KEY(key))
    """,
    )
    instance.query(
        f"INSERT INTO postgres_database.{table} SELECT number, [1, 2] from numbers(0, 1000000)"
    )

    # Create a table with wrong table structure
    assert "Could not convert string to i" in instance.query_and_get_error(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE {table} (a Int32, b Int32) ENGINE=MaterializedPostgreSQL('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres_database', '{table}', 'postgres', '{pg_pass}') ORDER BY a
        """
    )


def test_symbols_in_publication_name(started_cluster):
    id = uuid.uuid4()
    db = f"test_{id}"
    table = "test_symbols_in_publication_name"

    pg_manager3 = PostgresManager()
    pg_manager3.init(
        instance,
        cluster.postgres_ip,
        cluster.postgres_port,
        default_database=db,
    )

    pg_manager3.create_postgres_table(table)
    instance.query(
        f"INSERT INTO `{db}`.`{table}` SELECT number, number from numbers(0, 50)"
    )

    pg_manager3.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        materialized_database=db,
        settings=[
            f"materialized_postgresql_tables_list = '{table}'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    check_tables_are_synchronized(
        instance, table, materialized_database=db, postgres_database=db
    )
    pg_manager3.drop_materialized_db(db)
    pg_manager3.execute(f'drop table "{table}"')


def test_generated_columns(started_cluster):
    table = "test_generated_columns"

    pg_manager.create_postgres_table(
        table,
        "",
        f"""CREATE TABLE {table} (
             key integer PRIMARY KEY,
             x integer DEFAULT 0,
             temp integer DEFAULT 0,
             y integer GENERATED ALWAYS AS (x*2) STORED,
             z text DEFAULT 'z');
         """,
    )

    pg_manager.execute(f"alter table {table} drop column temp;")
    pg_manager.execute(f"insert into {table} (key, x, z) values (1,1,'1');")
    pg_manager.execute(f"insert into {table} (key, x, z) values (2,2,'2');")

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_tables_list = '{table}'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )

    pg_manager.execute(f"insert into {table} (key, x, z) values (3,3,'3');")
    pg_manager.execute(f"insert into {table} (key, x, z) values (4,4,'4');")

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )

    pg_manager.execute(f"insert into {table} (key, x, z) values (5,5,'5');")
    pg_manager.execute(f"insert into {table} (key, x, z) values (6,6,'6');")

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )


def test_generated_columns_with_sequence(started_cluster):
    table = "test_generated_columns_with_sequence"

    pg_manager.create_postgres_table(
        table,
        "",
        f"""CREATE TABLE {table} (
             key integer PRIMARY KEY,
             x integer,
             y integer GENERATED ALWAYS AS (x*2) STORED,
             z text);
         """,
    )

    pg_manager.execute(
        f"create sequence {table}_id_seq increment by 1 minvalue 1 start 1;"
    )
    pg_manager.execute(
        f"alter table {table} alter key set default nextval('{table}_id_seq');"
    )
    pg_manager.execute(f"insert into {table} (key, x, z) values (1,1,'1');")
    pg_manager.execute(f"insert into {table} (key, x, z) values (2,2,'2');")

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_tables_list = '{table}'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )


def test_default_columns(started_cluster):
    table = "test_default_columns"

    pg_manager.create_postgres_table(
        table,
        "",
        f"""CREATE TABLE {table} (
             key integer PRIMARY KEY,
             x integer,
             y text DEFAULT 'y1',
             z integer,
             a text DEFAULT 'a1',
             b integer);
         """,
    )

    pg_manager.execute(f"insert into {table} (key, x, z, b) values (1,1,1,1);")
    pg_manager.execute(f"insert into {table} (key, x, z, b) values (2,2,2,2);")

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_tables_list = '{table}'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )

    pg_manager.execute(f"insert into {table} (key, x, z, b) values (3,3,3,3);")
    pg_manager.execute(f"insert into {table} (key, x, z, b) values (4,4,4,4);")

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )

    pg_manager.execute(f"insert into {table} (key, x, z, b) values (5,5,5,5);")
    pg_manager.execute(f"insert into {table} (key, x, z, b) values (6,6,6,6);")

    check_tables_are_synchronized(
        instance, table, postgres_database=pg_manager.get_default_database()
    )


def test_dependent_loading(started_cluster):
    table = "test_dependent_loading"

    pg_manager.create_postgres_table(table)
    instance.query(
        f"INSERT INTO postgres_database.{table} SELECT number, number from numbers(0, 50)"
    )

    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE {table} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres_database', '{table}', 'postgres', '{pg_pass}') ORDER BY key
        """
    )

    check_tables_are_synchronized(
        instance,
        table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    assert 50 == int(instance.query(f"SELECT count() FROM {table}"))

    instance.restart_clickhouse()

    check_tables_are_synchronized(
        instance,
        table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    assert 50 == int(instance.query(f"SELECT count() FROM {table}"))

    uuid = instance.query(
        f"SELECT uuid FROM system.tables WHERE name='{table}' and database='default' limit 1"
    ).strip()
    nested_table = f"default.`{uuid}_nested`"
    instance.contains_in_log(
        f"Table default.{table} has 1 dependencies: {nested_table} (level 1)"
    )

    instance.query("SYSTEM FLUSH LOGS")
    nested_time = instance.query(
        f"SELECT event_time_microseconds FROM system.text_log WHERE message like 'Loading table default.{uuid}_nested' and message not like '%like%'"
    ).strip()
    time = (
        instance.query(
            f"SELECT event_time_microseconds FROM system.text_log WHERE message like 'Loading table default.{table}' and message not like '%like%'"
        )
        .strip()
        .split("\n")[-1]
    )
    instance.query(
        f"SELECT toDateTime64('{nested_time}', 6) < toDateTime64('{time}', 6)"
    )

    instance.query(f"DROP TABLE {table} SYNC")


def test_partial_table(started_cluster):
    table = "test_partial_table"

    pg_manager.create_postgres_table(
        table,
        "",
        f"""CREATE TABLE {table} (
             key integer PRIMARY KEY,
             x integer DEFAULT 0,
             y integer,
             z text DEFAULT 'z');
         """,
    )
    pg_manager.execute(f"insert into {table} (key, x, z) values (1,1,'a');")
    pg_manager.execute(f"insert into {table} (key, x, z) values (2,2,'b');")

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_tables_list = '{table}(z, key)'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    check_tables_are_synchronized(
        instance,
        table,
        postgres_database=pg_manager.get_default_database(),
        columns=["key", "z"],
    )

    pg_manager.execute(f"insert into {table} (key, x, z) values (3,3,'c');")
    pg_manager.execute(f"insert into {table} (key, x, z) values (4,4,'d');")

    check_tables_are_synchronized(
        instance,
        table,
        postgres_database=pg_manager.get_default_database(),
        columns=["key", "z"],
    )


# it is important to check case when table name, with subset of columns, is substring of table with full set of columns
@pytest.mark.parametrize(
    "table_list",
    [
        "{}(key, x, z), {}_full, {}_full1",
        "{}_full, {}(key, x, z), {}_full1",
        "{}_full,{}(key, x, z),{}_full1",
        "{}_full,{}_full1,{}(key, x, z)",
    ],
)
def test_partial_and_full_table(started_cluster, table_list):
    table = "test_partial_and_full_table"
    table_list = table_list.format(table, table, table)
    print(table_list)

    pg_manager.create_postgres_table(
        table,
        "",
        f"""CREATE TABLE {table} (
             key integer PRIMARY KEY,
             x integer DEFAULT 0,
             y integer,
             z text DEFAULT 'z');
         """,
    )
    pg_manager.execute(f"insert into {table} (key, x, y, z) values (1,1,1,'1');")
    pg_manager.execute(f"insert into {table} (key, x, y, z) values (2,2,2,'2');")

    pg_manager.create_postgres_table(
        table + "_full",
        "",
        f"""CREATE TABLE {table}_full (
             key integer PRIMARY KEY,
             x integer DEFAULT 0,
             y integer,
             z text DEFAULT 'z');
         """,
    )
    pg_manager.execute(f"insert into {table}_full (key, x, y, z) values (3,3,3,'3');")
    pg_manager.execute(f"insert into {table}_full (key, x, y, z) values (4,4,4,'4');")

    pg_manager.create_postgres_table(
        table + "_full1",
        "",
        f"""CREATE TABLE {table}_full1 (
             key integer PRIMARY KEY,
             x integer DEFAULT 0,
             y integer,
             z text DEFAULT 'z');
         """,
    )
    pg_manager.execute(f"insert into {table}_full1 (key, x, y, z) values (5,5,5,'5');")
    pg_manager.execute(f"insert into {table}_full1 (key, x, y, z) values (6,6,6,'6');")

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_tables_list = '{table_list}'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    check_tables_are_synchronized(
        instance,
        f"{table}",
        postgres_database=pg_manager.get_default_database(),
        columns=["key", "x", "z"],
    )
    check_tables_are_synchronized(
        instance, f"{table}_full", postgres_database=pg_manager.get_default_database()
    )
    check_tables_are_synchronized(
        instance, f"{table}_full1", postgres_database=pg_manager.get_default_database()
    )

    pg_manager.execute(f"insert into {table} (key, x, z) values (7,7,'7');")
    pg_manager.execute(f"insert into {table}_full (key, x, z) values (8,8,'8');")
    pg_manager.execute(f"insert into {table}_full1 (key, x, z) values (9,9,'9');")

    check_tables_are_synchronized(
        instance,
        f"{table}",
        postgres_database=pg_manager.get_default_database(),
        columns=["key", "x", "z"],
    )
    check_tables_are_synchronized(
        instance, f"{table}_full", postgres_database=pg_manager.get_default_database()
    )
    check_tables_are_synchronized(
        instance, f"{table}_full1", postgres_database=pg_manager.get_default_database()
    )


def test_quoting_publication(started_cluster):
    postgres_database = "postgres-postgres"
    pg_manager3 = PostgresManager()
    pg_manager3.init(
        instance,
        cluster.postgres_ip,
        cluster.postgres_port,
        default_database=postgres_database,
    )
    NUM_TABLES = 5
    materialized_database = "test-database"

    pg_manager3.create_and_fill_postgres_tables(NUM_TABLES, 10000)

    check_table_name_1 = "postgresql-replica-5"
    pg_manager3.create_and_fill_postgres_table(check_table_name_1)

    pg_manager3.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        materialized_database=materialized_database,
    )
    check_several_tables_are_synchronized(
        instance,
        NUM_TABLES,
        materialized_database=materialized_database,
        postgres_database=postgres_database,
    )

    result = instance.query(f"SHOW TABLES FROM `{materialized_database}`")
    assert (
        result
        == "postgresql-replica-5\npostgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\n"
    )

    check_tables_are_synchronized(
        instance,
        check_table_name_1,
        materialized_database=materialized_database,
        postgres_database=postgres_database,
    )
    instance.query(
        f"INSERT INTO `{postgres_database}`.`{check_table_name_1}` SELECT number, number from numbers(10000, 10000)"
    )
    check_tables_are_synchronized(
        instance,
        check_table_name_1,
        materialized_database=materialized_database,
        postgres_database=postgres_database,
    )

    check_table_name_2 = "postgresql-replica-6"
    pg_manager3.create_and_fill_postgres_table(check_table_name_2)

    instance.query(f"ATTACH TABLE `{materialized_database}`.`{check_table_name_2}`")

    result = instance.query(f"SHOW TABLES FROM `{materialized_database}`")
    assert (
        result
        == "postgresql-replica-5\npostgresql-replica-6\npostgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\n"
    )

    check_tables_are_synchronized(
        instance,
        check_table_name_2,
        materialized_database=materialized_database,
        postgres_database=postgres_database,
    )
    instance.query(
        f"INSERT INTO `{postgres_database}`.`{check_table_name_2}` SELECT number, number from numbers(10000, 10000)"
    )
    check_tables_are_synchronized(
        instance,
        check_table_name_2,
        materialized_database=materialized_database,
        postgres_database=postgres_database,
    )

    instance.restart_clickhouse()
    check_tables_are_synchronized(
        instance,
        check_table_name_1,
        materialized_database=materialized_database,
        postgres_database=postgres_database,
    )
    check_tables_are_synchronized(
        instance,
        check_table_name_2,
        materialized_database=materialized_database,
        postgres_database=postgres_database,
    )

    instance.query(
        f"DETACH TABLE `{materialized_database}`.`{check_table_name_2}` PERMANENTLY"
    )
    time.sleep(5)

    result = instance.query(f"SHOW TABLES FROM `{materialized_database}`")
    assert (
        result
        == "postgresql-replica-5\npostgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\n"
    )


def test_use_extended_date_and_time_types_setting(started_cluster):
    # https://github.com/ClickHouse/ClickHouse/issues/43153
    # By default PostgreSQL `date`/`timestamp` map to ClickHouse Date32/DateTime64 (wider range).
    # materialized_postgresql_use_extended_date_and_time_types = 0 falls back to Date/DateTime,
    # recursing through Nullable and Array.
    cursor = pg_manager.get_db_cursor()
    cursor.execute("DROP TABLE IF EXISTS test_date_types")
    cursor.execute(
        "CREATE TABLE test_date_types ("
        "key integer PRIMARY KEY, d date, t timestamp, "
        "arr_d date[] NOT NULL, arr_t timestamp[] NOT NULL)"
    )
    cursor.execute(
        "INSERT INTO test_date_types VALUES "
        "(1, '2000-05-12', '2000-05-12 12:12:12.012345', "
        "'{2000-05-12,2001-01-01}', '{2000-05-12 12:12:12}')"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_tables_list = 'test_date_types'",
            "materialized_postgresql_use_extended_date_and_time_types = 0",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    assert_eq_with_retry(
        instance, "SELECT count() FROM test_database.test_date_types", "1"
    )

    def col_type(name):
        return instance.query(
            "SELECT type FROM system.columns WHERE database='test_database' "
            f"AND table='test_date_types' AND name='{name}'"
        ).strip()

    # Narrowing must have happened for every column (scalar, nullable and array).
    for name in ("d", "t", "arr_d", "arr_t"):
        typ = col_type(name)
        assert "Date32" not in typ and "DateTime64" not in typ, f"{name}: {typ}"

    assert "Nullable(Date)" == col_type("d")
    assert "Nullable(DateTime)" == col_type("t")
    assert col_type("arr_d").startswith("Array(") and "Date" in col_type("arr_d")
    assert col_type("arr_t").startswith("Array(") and "DateTime" in col_type("arr_t")

    assert "2000-05-12\t2000-05-12 12:12:12" == instance.query(
        "SELECT d, t FROM test_database.test_date_types WHERE key = 1"
    ).strip()

    # Ongoing replication must keep working with the narrower types.
    cursor.execute(
        "INSERT INTO test_date_types VALUES "
        "(2, '2020-01-01', '2020-01-01 01:02:03', '{2020-01-01}', '{2020-01-01 01:02:03}')"
    )
    assert_eq_with_retry(
        instance, "SELECT count() FROM test_database.test_date_types", "2"
    )

    # Verify the replicated values, not just the row count. MaterializedPostgreSQLConsumer
    # catches conversion failures and still inserts the row with default values, so a broken
    # WAL decoder for the narrowed Date/DateTime or array types would pass a count-only check.
    assert (
        "2020-01-01\t2020-01-01 01:02:03\t['2020-01-01']\t['2020-01-01 01:02:03']"
        == instance.query(
            "SELECT d, t, arr_d, arr_t FROM test_database.test_date_types WHERE key = 2"
        ).strip()
    )

    pg_manager.drop_materialized_db()
    cursor.execute("DROP TABLE IF EXISTS test_date_types")


def test_use_extended_date_and_time_types_setting_table_engine_rejected(started_cluster):
    # The setting only affects the MaterializedPostgreSQL database engine, where the nested
    # table structure is derived from PostgreSQL. For the table engine the user declares the
    # column types explicitly, so the setting cannot have any effect and must be rejected with
    # a clear exception rather than silently ignored.
    table = "test_date_types_table_engine"
    error = instance.query_and_get_error(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE {table} (key Int32, d Date)
        ENGINE=MaterializedPostgreSQL('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres_database', '{table}', 'postgres', '{pg_pass}')
        ORDER BY key
        SETTINGS materialized_postgresql_use_extended_date_and_time_types = 0
        """
    )
    assert (
        "materialized_postgresql_use_extended_date_and_time_types" in error
        and "table engine" in error
    ), error


def test_use_extended_date_and_time_types_setting_alter_database_rejected(started_cluster):
    # The setting only controls the column types chosen by type inference when the nested tables
    # are created. The already created nested tables keep their fixed column types, so changing it
    # for an existing database with `ALTER DATABASE ... MODIFY SETTING` would be a silent no-op and
    # must be rejected with a clear exception instead.
    cursor = pg_manager.get_db_cursor()
    cursor.execute("DROP TABLE IF EXISTS test_alter_date_types")
    cursor.execute("CREATE TABLE test_alter_date_types (key integer PRIMARY KEY, d date)")
    cursor.execute("INSERT INTO test_alter_date_types VALUES (1, '2000-05-12')")

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_tables_list = 'test_alter_date_types'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    assert_eq_with_retry(
        instance, "SELECT count() FROM test_database.test_alter_date_types", "1"
    )

    error = instance.query_and_get_error(
        "ALTER DATABASE test_database MODIFY SETTING "
        "materialized_postgresql_use_extended_date_and_time_types = 0"
    )
    assert (
        "materialized_postgresql_use_extended_date_and_time_types" in error
        and "cannot be changed for an existing database" in error
    ), error

    pg_manager.drop_materialized_db()
    cursor.execute("DROP TABLE IF EXISTS test_alter_date_types")


def test_use_extended_date_and_time_types_setting_alter_database_atomic(started_cluster):
    # A multi-setting `ALTER DATABASE ... MODIFY SETTING` that rejects the immutable
    # `materialized_postgresql_use_extended_date_and_time_types` must validate the whole statement
    # before applying anything. Otherwise a mutable setting placed before the immutable one in the
    # same statement would already be applied (to the live replication handler, the in-memory settings
    # and the on-disk metadata) when the statement aborts, so the rejected alter could still change the
    # database. Here `materialized_postgresql_max_block_size` precedes the immutable setting and must
    # remain unchanged after the failed statement.
    cursor = pg_manager.get_db_cursor()
    cursor.execute("DROP TABLE IF EXISTS test_alter_atomic_date_types")
    cursor.execute(
        "CREATE TABLE test_alter_atomic_date_types (key integer PRIMARY KEY, d date)"
    )
    cursor.execute("INSERT INTO test_alter_atomic_date_types VALUES (1, '2000-05-12')")

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_tables_list = 'test_alter_atomic_date_types'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
            "materialized_postgresql_max_block_size = 11111",
        ],
    )
    assert_eq_with_retry(
        instance,
        "SELECT count() FROM test_database.test_alter_atomic_date_types",
        "1",
    )

    create_before = instance.query("SHOW CREATE DATABASE test_database")
    assert "materialized_postgresql_max_block_size = 11111" in create_before, create_before

    # The mutable `materialized_postgresql_max_block_size` precedes the immutable setting, so a
    # non-atomic implementation would apply it before throwing on the immutable one.
    error = instance.query_and_get_error(
        "ALTER DATABASE test_database MODIFY SETTING "
        "materialized_postgresql_max_block_size = 22222, "
        "materialized_postgresql_use_extended_date_and_time_types = 0"
    )
    assert (
        "materialized_postgresql_use_extended_date_and_time_types" in error
        and "cannot be changed for an existing database" in error
    ), error

    # The rejected statement must not have changed the mutable setting.
    create_after = instance.query("SHOW CREATE DATABASE test_database")
    assert "materialized_postgresql_max_block_size = 11111" in create_after, create_after
    assert "22222" not in create_after, create_after

    # A valid `ALTER DATABASE` of the mutable setting alone still works and is persisted, so the
    # added pre-validation pass did not break the normal path.
    instance.query(
        "ALTER DATABASE test_database MODIFY SETTING materialized_postgresql_max_block_size = 33333"
    )
    create_valid = instance.query("SHOW CREATE DATABASE test_database")
    assert "materialized_postgresql_max_block_size = 33333" in create_valid, create_valid

    pg_manager.drop_materialized_db()
    cursor.execute("DROP TABLE IF EXISTS test_alter_atomic_date_types")


def test_table_schema_changed_while_server_down(started_cluster):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/66273:
    # when the structure of a replicated PostgreSQL table changes while it is not observed
    # through the replication stream (e.g. the change happens and then the server restarts),
    # MaterializedPostgreSQL used to abort startup of the whole database with
    # `LOGICAL_ERROR: Columns number mismatch`, stopping replication of *all* tables.
    # Now only the affected table is skipped, the rest keep replicating, and the affected
    # table can be brought back with DETACH/ATTACH.
    NUM_TABLES = 3
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES, numbers=10)
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    check_several_tables_are_synchronized(instance, NUM_TABLES)

    # Change the structure of one table in PostgreSQL and restart ClickHouse, so that the
    # change is detected during startup rather than through the replication stream.
    pg_manager.execute("ALTER TABLE postgresql_replica_0 ADD COLUMN col_added integer")
    instance.restart_clickhouse()

    # The other tables must keep replicating despite the structure mismatch on replica_0.
    for i in range(1, NUM_TABLES):
        instance.query(
            f"INSERT INTO postgres_database.postgresql_replica_{i} SELECT number, number from numbers(10, 10)"
        )
    for i in range(1, NUM_TABLES):
        check_tables_are_synchronized(instance, f"postgresql_replica_{i}")

    # Write to the affected table while it is skipped. The consumer observes its `Relation` message,
    # finds no storage for it, and puts the table into the internal skip list (an empty start-LSN
    # entry keyed by the PostgreSQL relation id). The DETACH/ATTACH recovery below must clear that
    # entry, otherwise replication of the table would stay blocked forever after recovery.
    instance.query(
        "INSERT INTO postgres_database.postgresql_replica_0 (key, value, col_added) SELECT number, number, number from numbers(100, 5)"
    )
    # Wait until the table is actually marked as skipped in the replication stream, so the recovery
    # really exercises the stale-skip-list path rather than winning a benign race.
    instance.wait_for_log_line(
        "postgresql_replica_0 is skipped from replication stream", timeout=60
    )

    # The affected table is recoverable with DETACH/ATTACH (it picks up the new structure).
    instance.query("DETACH TABLE test_database.postgresql_replica_0 PERMANENTLY")
    instance.query("ATTACH TABLE test_database.postgresql_replica_0")
    assert_number_of_columns(instance, 3, "postgresql_replica_0")
    check_tables_are_synchronized(instance, "postgresql_replica_0")

    # Prove that replication really resumes for the recovered table: a write that happens *after*
    # ATTACH must be replicated. Without clearing the stale skip-list entry, this insert would be
    # skipped indefinitely and the tables would never converge.
    instance.query(
        "INSERT INTO postgres_database.postgresql_replica_0 (key, value, col_added) SELECT number, number, number from numbers(200, 5)"
    )
    check_tables_are_synchronized(instance, "postgresql_replica_0")

    pg_manager.drop_materialized_db()


def test_numeric_to_int256(started_cluster):
    # https://github.com/ClickHouse/ClickHouse/issues/59224
    # PostgreSQL numeric with precision wider than Decimal256 can hold (76 digits) and scale 0
    # (e.g. numeric(78,0), used to store 256-bit integers) is mapped to ClickHouse Int256.
    cursor = pg_manager.get_db_cursor()
    cursor.execute("DROP TABLE IF EXISTS test_int256")
    cursor.execute(
        "CREATE TABLE test_int256 "
        "(key integer PRIMARY KEY, v numeric(78, 0) NOT NULL, varr numeric(78, 0)[] NOT NULL)"
    )
    # Int256 max value (77 digits), a negative value and zero.
    int256_max = "57896044618658097711785492504343953926634992332820282019728792003956564819967"
    cursor.execute(
        f"INSERT INTO test_int256 VALUES "
        f"(1, {int256_max}, '{{{int256_max},-1}}'), (2, -12345678901234567890123456789, '{{0}}'), (3, 0, '{{1,2}}')"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_tables_list = 'test_int256'",
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    assert_eq_with_retry(
        instance, "SELECT count() FROM test_database.test_int256", "3"
    )

    assert "Int256" == instance.query(
        "SELECT type FROM system.columns WHERE database='test_database' "
        "AND table='test_int256' AND name='v'"
    ).strip()
    assert "Array(Int256)" == instance.query(
        "SELECT type FROM system.columns WHERE database='test_database' "
        "AND table='test_int256' AND name='varr'"
    ).strip()
    assert int256_max == instance.query(
        "SELECT v FROM test_database.test_int256 WHERE key = 1"
    ).strip()
    assert "-12345678901234567890123456789" == instance.query(
        "SELECT v FROM test_database.test_int256 WHERE key = 2"
    ).strip()
    assert f"[{int256_max},-1]" == instance.query(
        "SELECT varr FROM test_database.test_int256 WHERE key = 1"
    ).strip()

    # Ongoing replication must work too.
    cursor.execute(f"INSERT INTO test_int256 VALUES (4, {int256_max}, '{{7,8}}')")
    assert_eq_with_retry(
        instance, "SELECT count() FROM test_database.test_int256", "4"
    )

    pg_manager.drop_materialized_db()
    cursor.execute("DROP TABLE IF EXISTS test_int256")


def test_numeric_int256_validation(started_cluster):
    # https://github.com/ClickHouse/ClickHouse/issues/59224
    # Regressions for the numeric -> Int256 mapping. The postgresql() table function is used
    # because it runs schema inference (fetchPostgreSQLTableStructure) and value parsing
    # (insertPostgreSQLValue) synchronously, so both error paths surface to the client.
    cursor = pg_manager.get_db_cursor()

    def pg_table(table_name):
        return (
            f"postgresql('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', "
            f"'postgres_database', '{table_name}', 'postgres', '{pg_pass}')"
        )

    # A numeric with scale greater than precision (allowed by PostgreSQL >= 15) must still be
    # rejected, as it was before the Int256 mapping was added: it cannot be a valid Decimal.
    cursor.execute("DROP TABLE IF EXISTS test_bad_scale")
    cursor.execute("CREATE TABLE test_bad_scale (key integer PRIMARY KEY, v numeric(5, 7))")
    error = instance.query_and_get_error(f"DESCRIBE TABLE {pg_table('test_bad_scale')}")
    assert "larger than precision" in error, error

    # A value that fits into numeric(78, 0) but is out of the Int256 range must be rejected
    # instead of being silently wrapped around (wide-integer text parsing does not detect overflow).
    cursor.execute("DROP TABLE IF EXISTS test_overflow")
    cursor.execute("CREATE TABLE test_overflow (key integer PRIMARY KEY, v numeric(78, 0))")
    # 10^77 has 78 digits (fits numeric(78, 0)) and exceeds the Int256 maximum (~5.79 * 10^76).
    cursor.execute(
        "INSERT INTO test_overflow VALUES "
        "(1, 100000000000000000000000000000000000000000000000000000000000000000000000000000)"
    )
    error = instance.query_and_get_error(f"SELECT v FROM {pg_table('test_overflow')}")
    assert "out of range of Int256" in error, error

    cursor.execute("DROP TABLE IF EXISTS test_bad_scale")
    cursor.execute("DROP TABLE IF EXISTS test_overflow")


def test_aggregating_materialized_view(started_cluster):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/39805:
    # creating an aggregating materialized view on top of a MaterializedPostgreSQL table
    # used to break replication (`DB::Exception: Too large size passed to allocator`).
    # Replication must keep working and the materialized view must keep receiving updates.
    pg_manager.execute("DROP TABLE IF EXISTS test_mv_agg")
    pg_manager.execute(
        "CREATE TABLE test_mv_agg (key integer PRIMARY KEY, name text NOT NULL, num integer)"
    )
    pg_manager.execute(
        "INSERT INTO test_mv_agg VALUES (1, 'a', 1), (2, 'b', 2), (3, 'a', 3)"
    )

    instance.query("DROP DATABASE IF EXISTS test_database")
    instance.query(
        "CREATE DATABASE test_database ENGINE = MaterializedPostgreSQL(postgres1) "
        "SETTINGS materialized_postgresql_tables_list='test_mv_agg'"
    )
    check_tables_are_synchronized(instance, "test_mv_agg")

    instance.query("DROP TABLE IF EXISTS mv_agg")
    instance.query(
        "CREATE MATERIALIZED VIEW mv_agg ENGINE = MergeTree ORDER BY name POPULATE AS "
        "SELECT name, sum(num) AS total FROM test_database.test_mv_agg GROUP BY name"
    )
    assert "a\t4\nb\t2" == instance.query(
        "SELECT name, sum(total) FROM mv_agg GROUP BY name ORDER BY name"
    ).strip()

    # Inserting after the view exists must not break replication of the underlying table.
    pg_manager.execute("INSERT INTO test_mv_agg VALUES (4, 'a', 10), (5, 'c', 5)")
    check_tables_are_synchronized(instance, "test_mv_agg")
    assert 5 == int(instance.query("SELECT count() FROM test_database.test_mv_agg"))

    # ... and the materialized view must reflect the newly replicated rows.
    assert "a\t14\nb\t2\nc\t5" == instance.query(
        "SELECT name, sum(total) FROM mv_agg GROUP BY name ORDER BY name"
    ).strip()

    instance.query("DROP VIEW mv_agg")
    pg_manager.drop_materialized_db()
    pg_manager.execute("DROP TABLE IF EXISTS test_mv_agg")


def test_uppercase_database_name(started_cluster):
    # Reproduces https://github.com/ClickHouse/ClickHouse/issues/64891 (and #64615):
    # a PostgreSQL database name with upper-case letters produced a publication name
    # with upper-case letters, but the `pgoutput` plugin folds the `publication_names`
    # option to lower case, so the consumer failed with
    # `publication "..._ch_publication" does not exist` and ongoing changes were not replicated.
    id = str(uuid.uuid4()).replace("-", "_")
    postgres_db = f"Test_Uppercase_{id}"
    materialized_db = f"materialized_{id}"
    table = "test_uppercase_table"

    pg_manager3 = PostgresManager()
    pg_manager3.init(
        instance,
        cluster.postgres_ip,
        cluster.postgres_port,
        default_database=postgres_db,
    )

    pg_manager3.create_postgres_table(table)
    instance.query(
        f"INSERT INTO `{postgres_db}`.`{table}` SELECT number, number from numbers(0, 50)"
    )

    pg_manager3.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        materialized_database=materialized_db,
        postgres_database=postgres_db,
        settings=[
            "materialized_postgresql_backoff_min_ms = 100",
            "materialized_postgresql_backoff_max_ms = 100",
        ],
    )
    check_tables_are_synchronized(
        instance,
        table,
        materialized_database=materialized_db,
        postgres_database=postgres_db,
    )

    # The failure only manifested for ongoing replication (the consumer path), so insert
    # more rows after the initial snapshot and verify they are replicated as well.
    instance.query(
        f"INSERT INTO `{postgres_db}`.`{table}` SELECT number, number from numbers(50, 50)"
    )
    check_tables_are_synchronized(
        instance,
        table,
        materialized_database=materialized_db,
        postgres_database=postgres_db,
    )

    pg_manager3.drop_materialized_db(materialized_db)
    pg_manager3.clear()


def test_uppercase_table_name_single_storage(started_cluster):
    # Companion to `test_uppercase_database_name` for the single-table `MaterializedPostgreSQL`
    # storage path (not the database engine). That path sets `materialized_postgresql_tables_list`
    # to the raw remote table name and never goes through the quoting pass of `fetchRequiredTables`,
    # so `CREATE PUBLICATION ... FOR TABLE ONLY <name>` referenced an unquoted, upper-case table.
    # PostgreSQL folds the unquoted identifier to lower case, the relation is not found, and the
    # `CREATE TABLE` fails before replication can start. The remote table name must be quoted.
    table = "Test_Uppercase_Table"

    pg_manager.create_postgres_table(table)
    instance.query(
        f"INSERT INTO postgres_database.`{table}` SELECT number, number from numbers(0, 50)"
    )

    instance.query(f"DROP TABLE IF EXISTS `{table}` SYNC")
    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE `{table}` (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres_database', '{table}', 'postgres', '{pg_pass}') ORDER BY key
        """
    )

    check_tables_are_synchronized(
        instance,
        table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    # Also verify ongoing replication after the initial snapshot.
    instance.query(
        f"INSERT INTO postgres_database.`{table}` SELECT number, number from numbers(50, 50)"
    )
    check_tables_are_synchronized(
        instance,
        table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    instance.query(f"DROP TABLE IF EXISTS `{table}` SYNC")
    pg_manager.execute(f'DROP TABLE "{table}"')


def test_publication_name_case_collision_single_storage(started_cluster):
    # Two PostgreSQL tables in the same database whose names differ only by case must each get their
    # own publication and replicate independently. Folding the publication name to lower case would
    # make `"Pub_Case_Collision"` and `"pub_case_collision"` collide on a single `..._ch_publication`
    # (the second `CREATE` dropping/recreating the first), diverging the first table's ongoing
    # replication. The publication name is kept case-preserving (and quoted when handed to the
    # `pgoutput` plugin), so there is no collision. A unique replication consumer identifier is used
    # so the (always lower-cased) replication slot names do not collide for this same-case-fold pair.
    # Related: https://github.com/ClickHouse/ClickHouse/issues/64891
    upper = "Pub_Case_Collision"
    lower = "pub_case_collision"

    # Disjoint key ranges per table, so a publication collision (one table's rows leaking into the
    # other, or replication stalling) would be visible as a synchronization mismatch.
    initial = {upper: (0, 50), lower: (1000, 50)}
    ongoing = {upper: (50, 50), lower: (1050, 50)}

    for name in (upper, lower):
        pg_manager.create_postgres_table(name)
        start, count = initial[name]
        instance.query(
            f"INSERT INTO postgres_database.`{name}` SELECT number, number from numbers({start}, {count})"
        )

        instance.query(f"DROP TABLE IF EXISTS `{name}` SYNC")
        instance.query(
            f"""
            SET allow_experimental_materialized_postgresql_table=1;
            CREATE TABLE `{name}` (key Int32, value Int32)
            ENGINE=MaterializedPostgreSQL('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres_database', '{name}', 'postgres', '{pg_pass}')
            ORDER BY key
            SETTINGS materialized_postgresql_use_unique_replication_consumer_identifier = 1
            """
        )

    for name in (upper, lower):
        check_tables_are_synchronized(
            instance,
            name,
            postgres_database=pg_manager.get_default_database(),
            materialized_database="default",
        )

    # Ongoing replication for both must keep working after the initial snapshots (the path that a
    # publication collision would break for the table whose publication was dropped).
    for name in (upper, lower):
        start, count = ongoing[name]
        instance.query(
            f"INSERT INTO postgres_database.`{name}` SELECT number, number from numbers({start}, {count})"
        )

    for name in (upper, lower):
        check_tables_are_synchronized(
            instance,
            name,
            postgres_database=pg_manager.get_default_database(),
            materialized_database="default",
        )

    for name in (upper, lower):
        instance.query(f"DROP TABLE IF EXISTS `{name}` SYNC")
        pg_manager.execute(f'DROP TABLE "{name}"')


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()

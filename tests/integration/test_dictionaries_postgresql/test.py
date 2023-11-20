import pytest
import time
import logging
import psycopg2
from multiprocessing.dummy import Pool

from helpers.cluster import ClickHouseCluster
from helpers.postgres_utility import get_postgres_conn
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.xml",
        "configs/dictionaries/postgres_dict.xml",
        "configs/named_collections.xml",
    ],
    with_postgres=True,
    with_postgres_cluster=True,
)


def create_postgres_db(conn, name):
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE {name}")


def create_postgres_table(cursor, table_name):
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    id Integer NOT NULL, key Integer NOT NULL, value Integer NOT NULL, PRIMARY KEY (id))
    """
    )


def create_and_fill_postgres_table(cursor, table_name, port, host):
    create_postgres_table(cursor, table_name)
    # Fill postgres table using clickhouse postgres table function and check
    table_func = f"""postgresql('{host}:{port}', 'postgres_database', '{table_name}', 'postgres', 'mysecretpassword')"""
    node1.query(
        f"""INSERT INTO TABLE FUNCTION {table_func} SELECT number, number, number from numbers(10000)"""
    )
    result = node1.query(f"SELECT count() FROM {table_func}")
    assert result.rstrip() == "10000"


def create_dict(table_name, index=0):
    node1.query(
        f"""
    CREATE TABLE IF NOT EXISTS `test`.`dict_table_{table_name}` (
        `key` UInt32, `value` UInt32
    ) ENGINE = Dictionary(dict{str(index)})
    """
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query("CREATE DATABASE IF NOT EXISTS test")

        postgres_conn = get_postgres_conn(
            ip=cluster.postgres_ip, port=cluster.postgres_port
        )
        logging.debug("postgres1 connected")
        create_postgres_db(postgres_conn, "postgres_database")

        postgres2_conn = get_postgres_conn(
            ip=cluster.postgres2_ip, port=cluster.postgres_port
        )
        logging.debug("postgres2 connected")
        create_postgres_db(postgres2_conn, "postgres_database")

        yield cluster

    finally:
        cluster.shutdown()


def test_load_dictionaries(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        database=True,
        port=started_cluster.postgres_port,
    )
    cursor = conn.cursor()
    table_name = "test0"
    create_and_fill_postgres_table(
        cursor,
        table_name,
        port=started_cluster.postgres_port,
        host=started_cluster.postgres_ip,
    )
    create_dict(table_name)
    dict_name = "dict0"

    node1.query(f"SYSTEM RELOAD DICTIONARY {dict_name}")
    assert (
        node1.query(f"SELECT count() FROM `test`.`dict_table_{table_name}`").rstrip()
        == "10000"
    )
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'key', toUInt64(0))") == "0\n"
    )
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(9999))")
        == "9999\n"
    )

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    node1.query(f"DROP DICTIONARY IF EXISTS {dict_name}")


def test_postgres_dictionaries_custom_query_full_load(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        database=True,
        port=started_cluster.postgres_port,
    )
    cursor = conn.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer, value_1 Text);"
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS test_table_2 (id Integer, value_2 Text);"
    )
    cursor.execute("INSERT INTO test_table_1 VALUES (1, 'Value_1');")
    cursor.execute("INSERT INTO test_table_2 VALUES (1, 'Value_2');")

    query = node1.query
    query(
        f"""
    CREATE DICTIONARY test_dictionary_custom_query
    (
        id UInt64,
        value_1 String,
        value_2 String
    )
    PRIMARY KEY id
    LAYOUT(FLAT())
    SOURCE(PostgreSQL(
        DB 'postgres_database'
        HOST '{started_cluster.postgres_ip}'
        PORT {started_cluster.postgres_port}
        USER 'postgres'
        PASSWORD 'mysecretpassword'
        QUERY $doc$SELECT id, value_1, value_2 FROM test_table_1 INNER JOIN test_table_2 USING (id);$doc$))
    LIFETIME(0)
    """
    )

    result = query("SELECT id, value_1, value_2 FROM test_dictionary_custom_query")

    assert result == "1\tValue_1\tValue_2\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

    cursor.execute("DROP TABLE test_table_2;")
    cursor.execute("DROP TABLE test_table_1;")


def test_postgres_dictionaries_custom_query_partial_load_simple_key(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        database=True,
        port=started_cluster.postgres_port,
    )
    cursor = conn.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer, value_1 Text);"
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS test_table_2 (id Integer, value_2 Text);"
    )
    cursor.execute("INSERT INTO test_table_1 VALUES (1, 'Value_1');")
    cursor.execute("INSERT INTO test_table_2 VALUES (1, 'Value_2');")

    query = node1.query
    query(
        f"""
    CREATE DICTIONARY test_dictionary_custom_query
    (
        id UInt64,
        value_1 String,
        value_2 String
    )
    PRIMARY KEY id
    LAYOUT(DIRECT())
    SOURCE(PostgreSQL(
        DB 'postgres_database'
        HOST '{started_cluster.postgres_ip}'
        PORT {started_cluster.postgres_port}
        USER 'postgres'
        PASSWORD 'mysecretpassword'
        QUERY $doc$SELECT id, value_1, value_2 FROM test_table_1 INNER JOIN test_table_2 USING (id) WHERE {{condition}};$doc$))
    """
    )

    result = query(
        "SELECT dictGet('test_dictionary_custom_query', ('value_1', 'value_2'), toUInt64(1))"
    )

    assert result == "('Value_1','Value_2')\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

    cursor.execute("DROP TABLE test_table_2;")
    cursor.execute("DROP TABLE test_table_1;")


def test_postgres_dictionaries_custom_query_partial_load_complex_key(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        database=True,
        port=started_cluster.postgres_port,
    )
    cursor = conn.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS test_table_1 (id Integer, key Text, value_1 Text);"
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS test_table_2 (id Integer, key Text, value_2 Text);"
    )
    cursor.execute("INSERT INTO test_table_1 VALUES (1, 'Key', 'Value_1');")
    cursor.execute("INSERT INTO test_table_2 VALUES (1, 'Key', 'Value_2');")

    query = node1.query
    query(
        f"""
    CREATE DICTIONARY test_dictionary_custom_query
    (
        id UInt64,
        key String,
        value_1 String,
        value_2 String
    )
    PRIMARY KEY id, key
    LAYOUT(COMPLEX_KEY_DIRECT())
    SOURCE(PostgreSQL(
        DB 'postgres_database'
        HOST '{started_cluster.postgres_ip}'
        PORT {started_cluster.postgres_port}
        USER 'postgres'
        PASSWORD 'mysecretpassword'
        QUERY $doc$SELECT id, key, value_1, value_2 FROM test_table_1 INNER JOIN test_table_2 USING (id, key) WHERE {{condition}};$doc$))
    """
    )

    result = query(
        "SELECT dictGet('test_dictionary_custom_query', ('value_1', 'value_2'), (toUInt64(1), 'Key'))"
    )

    assert result == "('Value_1','Value_2')\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

    cursor.execute("DROP TABLE test_table_2;")
    cursor.execute("DROP TABLE test_table_1;")


def test_invalidate_query(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        database=True,
        port=started_cluster.postgres_port,
    )
    cursor = conn.cursor()
    table_name = "test0"
    create_and_fill_postgres_table(
        cursor,
        table_name,
        port=started_cluster.postgres_port,
        host=started_cluster.postgres_ip,
    )

    # invalidate query: SELECT value FROM test0 WHERE id = 0
    dict_name = "dict0"
    create_dict(table_name)
    node1.query(f"SYSTEM RELOAD DICTIONARY {dict_name}")
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(0))")
        == "0\n"
    )
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(1))")
        == "1\n"
    )

    # update should happen
    cursor.execute(f"UPDATE {table_name} SET value=value+1 WHERE id = 0")
    while True:
        result = node1.query(
            f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(0))"
        )
        if result != "0\n":
            break
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(0))")
        == "1\n"
    )

    # no update should happen
    cursor.execute(f"UPDATE {table_name} SET value=value*2 WHERE id != 0")
    time.sleep(5)
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(0))")
        == "1\n"
    )
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(1))")
        == "1\n"
    )

    # update should happen
    cursor.execute(f"UPDATE {table_name} SET value=value+1 WHERE id = 0")
    time.sleep(5)
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(0))")
        == "2\n"
    )
    assert (
        node1.query(f"SELECT dictGetUInt32('{dict_name}', 'value', toUInt64(1))")
        == "2\n"
    )

    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    node1.query(f"DROP DICTIONARY IF EXISTS {dict_name}")
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_dictionary_with_replicas(started_cluster):
    conn1 = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor1 = conn1.cursor()
    conn2 = get_postgres_conn(
        ip=started_cluster.postgres2_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor2 = conn2.cursor()

    create_postgres_table(cursor1, "test1")
    create_postgres_table(cursor2, "test1")

    cursor1.execute(
        "INSERT INTO test1 select i, i, i from generate_series(0, 99) as t(i);"
    )
    cursor2.execute(
        "INSERT INTO test1 select i, i, i from generate_series(100, 199) as t(i);"
    )

    create_dict("test1", 1)
    result = node1.query("SELECT * FROM `test`.`dict_table_test1` ORDER BY key")

    # priority 0 - non running port
    assert node1.contains_in_log("PostgreSQLConnectionPool: Connection error*")

    # priority 1 - postgres2, table contains rows with values 100-200
    # priority 2 - postgres1, table contains rows with values 0-100
    expected = node1.query("SELECT number, number FROM numbers(100, 100)")
    assert result == expected

    cursor1.execute("DROP TABLE IF EXISTS test1")
    cursor2.execute("DROP TABLE IF EXISTS test1")

    node1.query("DROP TABLE IF EXISTS test1")
    node1.query("DROP DICTIONARY IF EXISTS dict1")


def test_postgres_schema(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.test_table (id integer, value integer)")
    cursor.execute(
        "INSERT INTO test_schema.test_table SELECT i, i FROM generate_series(0, 99) as t(i)"
    )

    node1.query(
        """
    DROP DICTIONARY IF EXISTS postgres_dict;
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(
        port 5432
        host 'postgres1'
        user  'postgres'
        password 'mysecretpassword'
        db 'postgres_database'
        table 'test_schema.test_table'))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )

    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(1))")
    assert int(result.strip()) == 1
    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(99))")
    assert int(result.strip()) == 99
    node1.query("DROP DICTIONARY IF EXISTS postgres_dict")
    cursor.execute("DROP TABLE test_schema.test_table")
    cursor.execute("DROP SCHEMA test_schema")


def test_predefined_connection_configuration(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS test_table")
    cursor.execute("CREATE TABLE test_table (id integer, value integer)")
    cursor.execute(
        "INSERT INTO test_table SELECT i, i FROM generate_series(0, 99) as t(i)"
    )

    node1.query(
        """
    DROP DICTIONARY IF EXISTS postgres_dict;
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(NAME postgres1))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(99))")
    assert int(result.strip()) == 99

    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")
    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.test_table (id integer, value integer)")
    cursor.execute(
        "INSERT INTO test_schema.test_table SELECT i, 100 FROM generate_series(0, 99) as t(i)"
    )

    node1.query(
        """
    DROP DICTIONARY postgres_dict;
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(NAME postgres1 SCHEMA test_schema))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(99))")
    assert int(result.strip()) == 100

    node1.query(
        """
    DROP DICTIONARY postgres_dict;
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(NAME postgres2))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(99))")
    assert int(result.strip()) == 100

    node1.query("DROP DICTIONARY postgres_dict")
    node1.query(
        """
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(NAME postgres4))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = node1.query_and_get_error(
        "SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(99))"
    )

    node1.query(
        """
    DROP DICTIONARY postgres_dict;
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(NAME postgres1 PORT 5432))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(99))")
    assert int(result.strip()) == 99


def test_bad_configuration(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()

    node1.query(
        """
    DROP DICTIONARY IF EXISTS postgres_dict;
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(
        port 5432
        host 'postgres1'
        user  'postgres'
        password 'mysecretpassword'
        dbbb 'postgres_database'
        table 'test_schema.test_table'))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )

    node1.query_and_get_error(
        "SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(1))"
    )
    assert node1.contains_in_log("Unexpected key `dbbb`")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()

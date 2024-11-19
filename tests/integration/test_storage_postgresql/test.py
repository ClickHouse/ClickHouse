import logging
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.postgres_utility import get_postgres_conn

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/settings.xml", "configs/users.xml"],
    with_postgres_cluster=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query("CREATE DATABASE test")
        node2.query("CREATE DATABASE test")
        # Wait for the PostgreSQL handler to start.
        # cluster.start waits until port 9000 becomes accessible.
        # Server opens the PostgreSQL compatibility port a bit later.
        node1.wait_for_log_line("PostgreSQL compatibility protocol")
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    print("PostgreSQL is available - running test")
    yield  # run test
    node1.query("DROP DATABASE test")
    node1.query("CREATE DATABASE test")


def test_postgres_select_insert(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    table_name = "test_many"
    table = f"""postgresql('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres', '{table_name}', 'postgres', 'mysecretpassword')"""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (a integer, b text, c integer)")

    result = node1.query(
        f"""
        INSERT INTO TABLE FUNCTION {table}
        SELECT number, concat('name_', toString(number)), 3 from numbers(10000)"""
    )
    check1 = f"SELECT count() FROM {table}"
    check2 = f"SELECT Sum(c) FROM {table}"
    check3 = f"SELECT count(c) FROM {table} WHERE a % 2 == 0"
    check4 = f"SELECT count() FROM {table} WHERE b LIKE concat('name_', toString(1))"
    assert (node1.query(check1)).rstrip() == "10000"
    assert (node1.query(check2)).rstrip() == "30000"
    assert (node1.query(check3)).rstrip() == "5000"
    assert (node1.query(check4)).rstrip() == "1"

    # Triggers issue https://github.com/ClickHouse/ClickHouse/issues/26088
    # for i in range(1, 1000):
    #     assert (node1.query(check1)).rstrip() == '10000', f"Failed on {i}"

    result = node1.query(
        f"""
        INSERT INTO TABLE FUNCTION {table}
        SELECT number, concat('name_', toString(number)), 3 from numbers(1000000)"""
    )
    check1 = f"SELECT count() FROM {table}"
    check2 = f"SELECT count() FROM (SELECT * FROM {table} LIMIT 10)"
    assert (node1.query(check1)).rstrip() == "1010000"
    assert (node1.query(check2)).rstrip() == "10"

    cursor.execute(f"DROP TABLE {table_name} ")


def test_postgres_addresses_expr(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    table_name = "test_table"
    table = f"""postgresql(`postgres5`)"""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (a integer, b text, c integer)")

    node1.query(
        f"""
        INSERT INTO TABLE FUNCTION {table}
        SELECT number, concat('name_', toString(number)), 3 from numbers(10000)"""
    )
    check1 = f"SELECT count() FROM {table}"
    check2 = f"SELECT Sum(c) FROM {table}"
    check3 = f"SELECT count(c) FROM {table} WHERE a % 2 == 0"
    check4 = f"SELECT count() FROM {table} WHERE b LIKE concat('name_', toString(1))"
    assert (node1.query(check1)).rstrip() == "10000"
    assert (node1.query(check2)).rstrip() == "30000"
    assert (node1.query(check3)).rstrip() == "5000"
    assert (node1.query(check4)).rstrip() == "1"

    cursor.execute(f"DROP TABLE {table_name} ")


def test_postgres_conversions(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS test_types")
    cursor.execute(f"DROP TABLE IF EXISTS test_array_dimensions")

    cursor.execute(
        """CREATE TABLE test_types (
        a smallint, b integer, c bigint, d real, e double precision, f serial, g bigserial,
        h timestamp, i date, j decimal(5, 3), k numeric, l boolean, "M" integer)"""
    )
    node1.query(
        """
        INSERT INTO TABLE FUNCTION postgresql('postgres1:5432', 'postgres', 'test_types', 'postgres', 'mysecretpassword') VALUES
        (-32768, -2147483648, -9223372036854775808, 1.12345, 1.1234567890, 2147483647, 9223372036854775807, '2000-05-12 12:12:12.012345', '2000-05-12', 22.222, 22.222, 1, 42)"""
    )
    result = node1.query(
        """
        SELECT a, b, c, d, e, f, g, h, i, j, toDecimal128(k, 3), l, "M" FROM postgresql('postgres1:5432', 'postgres', 'test_types', 'postgres', 'mysecretpassword')"""
    )
    assert (
        result
        == "-32768\t-2147483648\t-9223372036854775808\t1.12345\t1.123456789\t2147483647\t9223372036854775807\t2000-05-12 12:12:12.012345\t2000-05-12\t22.222\t22.222\t1\t42\n"
    )

    cursor.execute(
        "INSERT INTO test_types (l) VALUES (TRUE), (true), ('yes'), ('y'), ('1');"
    )
    cursor.execute(
        "INSERT INTO test_types (l) VALUES (FALSE), (false), ('no'), ('off'), ('0');"
    )
    expected = "1\n1\n1\n1\n1\n1\n0\n0\n0\n0\n0\n"
    result = node1.query(
        """SELECT l FROM postgresql('postgres1:5432', 'postgres', 'test_types', 'postgres', 'mysecretpassword')"""
    )
    assert result == expected

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS test_array_dimensions
           (
                a Date[] NOT NULL,                          -- Date
                b Timestamp[] NOT NULL,                     -- DateTime64(6)
                c real[][] NOT NULL,                        -- Float32
                d double precision[][] NOT NULL,            -- Float64
                e decimal(5, 5)[][][] NOT NULL,             -- Decimal32
                f integer[][][] NOT NULL,                   -- Int32
                g Text[][][][][] NOT NULL,                  -- String
                h Integer[][][],                            -- Nullable(Int32)
                i Char(2)[][][][],                          -- Nullable(String)
                j Char(2)[],                                -- Nullable(String)
                k UUID[],                                   -- Nullable(UUID)
                l UUID[][],                                 -- Nullable(UUID)
                "M" integer[] NOT NULL                      -- Int32 (mixed-case identifier)
           )"""
    )

    result = node1.query(
        """
        DESCRIBE TABLE postgresql('postgres1:5432', 'postgres', 'test_array_dimensions', 'postgres', 'mysecretpassword')"""
    )
    expected = (
        "a\tArray(Date)\t\t\t\t\t\n"
        "b\tArray(DateTime64(6))\t\t\t\t\t\n"
        "c\tArray(Array(Float32))\t\t\t\t\t\n"
        "d\tArray(Array(Float64))\t\t\t\t\t\n"
        "e\tArray(Array(Array(Decimal(5, 5))))\t\t\t\t\t\n"
        "f\tArray(Array(Array(Int32)))\t\t\t\t\t\n"
        "g\tArray(Array(Array(Array(Array(String)))))\t\t\t\t\t\n"
        "h\tArray(Array(Array(Nullable(Int32))))\t\t\t\t\t\n"
        "i\tArray(Array(Array(Array(Nullable(String)))))\t\t\t\t\t\n"
        "j\tArray(Nullable(String))\t\t\t\t\t\n"
        "k\tArray(Nullable(UUID))\t\t\t\t\t\n"
        "l\tArray(Array(Nullable(UUID)))\t\t\t\t\t\n"
        "M\tArray(Int32)"
        ""
    )
    assert result.rstrip() == expected

    node1.query(
        "INSERT INTO TABLE FUNCTION postgresql('postgres1:5432', 'postgres', 'test_array_dimensions', 'postgres', 'mysecretpassword') "
        "VALUES ("
        "['2000-05-12', '2000-05-12'], "
        "['2000-05-12 12:12:12.012345', '2000-05-12 12:12:12.012345'], "
        "[[1.12345], [1.12345], [1.12345]], "
        "[[1.1234567891], [1.1234567891], [1.1234567891]], "
        "[[[0.11111, 0.11111]], [[0.22222, 0.22222]], [[0.33333, 0.33333]]], "
        "[[[1, 1], [1, 1]], [[3, 3], [3, 3]], [[4, 4], [5, 5]]], "
        "[[[[['winx', 'winx', 'winx']]]]], "
        "[[[1, NULL], [NULL, 1]], [[NULL, NULL], [NULL, NULL]], [[4, 4], [5, 5]]], "
        "[[[[NULL]]]], "
        "[], "
        "['2a0c0bfc-4fec-4e32-ae3a-7fc8eea6626a', '42209d53-d641-4d73-a8b6-c038db1e75d6', NULL], "
        "[[NULL, '42209d53-d641-4d73-a8b6-c038db1e75d6'], ['2a0c0bfc-4fec-4e32-ae3a-7fc8eea6626a', NULL], [NULL, NULL]],"
        "[42, 42, 42]"
        ")"
    )

    result = node1.query(
        """
        SELECT * FROM postgresql('postgres1:5432', 'postgres', 'test_array_dimensions', 'postgres', 'mysecretpassword')"""
    )
    expected = (
        "['2000-05-12','2000-05-12']\t"
        "['2000-05-12 12:12:12.012345','2000-05-12 12:12:12.012345']\t"
        "[[1.12345],[1.12345],[1.12345]]\t"
        "[[1.1234567891],[1.1234567891],[1.1234567891]]\t"
        "[[[0.11111,0.11111]],[[0.22222,0.22222]],[[0.33333,0.33333]]]\t"
        "[[[1,1],[1,1]],[[3,3],[3,3]],[[4,4],[5,5]]]\t"
        "[[[[['winx','winx','winx']]]]]\t"
        "[[[1,NULL],[NULL,1]],[[NULL,NULL],[NULL,NULL]],[[4,4],[5,5]]]\t"
        "[[[[NULL]]]]\t"
        "[]\t"
        "['2a0c0bfc-4fec-4e32-ae3a-7fc8eea6626a','42209d53-d641-4d73-a8b6-c038db1e75d6',NULL]\t"
        "[[NULL,'42209d53-d641-4d73-a8b6-c038db1e75d6'],['2a0c0bfc-4fec-4e32-ae3a-7fc8eea6626a',NULL],[NULL,NULL]]\t"
        "[42,42,42]\n"
    )
    assert result == expected

    cursor.execute(f"DROP TABLE test_types")
    cursor.execute(f"DROP TABLE test_array_dimensions")


def test_postgres_array_ndim_error_messges(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()

    # cleanup
    cursor.execute("DROP VIEW  IF EXISTS array_ndim_view;")
    cursor.execute("DROP TABLE IF EXISTS array_ndim_table;")

    # setup
    cursor.execute(
        'CREATE TABLE array_ndim_table (x INTEGER, "Mixed-case with spaces" INTEGER[]);'
    )
    cursor.execute("CREATE VIEW  array_ndim_view AS SELECT * FROM array_ndim_table;")
    describe_table = """
    DESCRIBE TABLE postgresql(
        'postgres1:5432', 'postgres', 'array_ndim_view',
        'postgres', 'mysecretpassword'
    )
    """

    # View with array column cannot be empty. Should throw a useful error message.
    # (Cannot infer array dimension.)
    try:
        node1.query(describe_table)
        assert False
    except Exception as error:
        assert (
            "PostgreSQL relation containing arrays cannot be empty: array_ndim_view"
            in str(error)
        )

    # View cannot have empty array. Should throw useful error message.
    # (Cannot infer array dimension.)
    cursor.execute("TRUNCATE array_ndim_table;")
    cursor.execute("INSERT INTO array_ndim_table VALUES (1234, '{}');")
    try:
        node1.query(describe_table)
        assert False
    except Exception as error:
        assert (
            'PostgreSQL cannot infer dimensions of an empty array: array_ndim_view."Mixed-case with spaces"'
            in str(error)
        )

    # View cannot have NULL array value. Should throw useful error message.
    cursor.execute("TRUNCATE array_ndim_table;")
    cursor.execute("INSERT INTO array_ndim_table VALUES (1234, NULL);")
    try:
        node1.query(describe_table)
        assert False
    except Exception as error:
        assert (
            'PostgreSQL array cannot be NULL: array_ndim_view."Mixed-case with spaces"'
            in str(error)
        )

    # cleanup
    cursor.execute("DROP VIEW  IF EXISTS array_ndim_view;")
    cursor.execute("DROP TABLE IF EXISTS array_ndim_table;")


def test_non_default_schema(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_pg_table_schema")
    node1.query("DROP TABLE IF EXISTS test_pg_table_schema_with_dots")

    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")
    cursor.execute('DROP SCHEMA IF EXISTS "test.nice.schema" CASCADE')

    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.test_table (a integer)")
    cursor.execute(
        "INSERT INTO test_schema.test_table SELECT i FROM generate_series(0, 99) as t(i)"
    )

    node1.query(
        """
        CREATE TABLE test.test_pg_table_schema (a UInt32)
        ENGINE PostgreSQL('postgres1:5432', 'postgres', 'test_table', 'postgres', 'mysecretpassword', 'test_schema');
    """
    )

    result = node1.query("SELECT * FROM test.test_pg_table_schema")
    expected = node1.query("SELECT number FROM numbers(100)")
    assert result == expected

    parameters = "'postgres1:5432', 'postgres', 'test_table', 'postgres', 'mysecretpassword', 'test_schema'"
    table_function = f"postgresql({parameters})"
    table_engine = f"PostgreSQL({parameters})"
    result = node1.query(f"SELECT * FROM {table_function}")
    assert result == expected

    cursor.execute('''CREATE SCHEMA "test.nice.schema"''')
    cursor.execute("""CREATE TABLE "test.nice.schema"."test.nice.table" (a integer)""")
    cursor.execute(
        'INSERT INTO "test.nice.schema"."test.nice.table" SELECT i FROM generate_series(0, 99) as t(i)'
    )

    node1.query(
        """
        CREATE TABLE test.test_pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('postgres1:5432', 'postgres', 'test.nice.table', 'postgres', 'mysecretpassword', 'test.nice.schema');
    """
    )
    result = node1.query("SELECT * FROM test.test_pg_table_schema_with_dots")
    assert result == expected

    cursor.execute(
        'INSERT INTO "test_schema"."test_table" SELECT i FROM generate_series(100, 199) as t(i)'
    )
    result = node1.query(f"SELECT * FROM {table_function}")
    expected = node1.query("SELECT number FROM numbers(200)")
    assert result == expected

    node1.query(f"CREATE TABLE test.test_pg_auto_schema_engine ENGINE={table_engine}")
    node1.query(f"CREATE TABLE test.test_pg_auto_schema_function AS {table_function}")

    expected = "a\tNullable(Int32)\t\t\t\t\t\n"
    assert node1.query("DESCRIBE TABLE test.test_pg_auto_schema_engine") == expected
    assert node1.query("DESCRIBE TABLE test.test_pg_auto_schema_function") == expected

    cursor.execute("DROP SCHEMA test_schema CASCADE")
    cursor.execute('DROP SCHEMA "test.nice.schema" CASCADE')
    node1.query("DROP TABLE test.test_pg_table_schema")
    node1.query("DROP TABLE test.test_pg_table_schema_with_dots")
    node1.query("DROP TABLE test.test_pg_auto_schema_engine")
    node1.query("DROP TABLE test.test_pg_auto_schema_function")


def test_concurrent_queries(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=False
    )
    cursor = conn.cursor()
    database_name = "concurrent_test"

    cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cursor.execute(f"CREATE DATABASE {database_name}")
    conn = get_postgres_conn(
        started_cluster.postgres_ip,
        started_cluster.postgres_port,
        database=True,
        database_name=database_name,
    )
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test_table (key integer, value integer)")

    node1.query(
        f"""
        CREATE TABLE test.test_table (key UInt32, value UInt32)
        ENGINE = PostgreSQL(postgres1, database='{database_name}', table='test_table')
    """
    )

    node1.query(
        f"""
        CREATE TABLE test.stat (numbackends UInt32, datname String)
        ENGINE = PostgreSQL(postgres1, database='{database_name}', table='pg_stat_database')
    """
    )

    def node_select(_):
        for i in range(20):
            result = node1.query("SELECT * FROM test.test_table", user="default")

    def node_insert(_):
        for i in range(20):
            result = node1.query(
                "INSERT INTO test.test_table SELECT number, number FROM numbers(1000)",
                user="default",
            )

    def node_insert_select(_):
        for i in range(20):
            result = node1.query(
                "INSERT INTO test.test_table SELECT number, number FROM numbers(1000)",
                user="default",
            )
            result = node1.query(
                "SELECT * FROM test.test_table LIMIT 100", user="default"
            )

    busy_pool = Pool(30)
    p = busy_pool.map_async(node_select, range(30))
    p.wait()

    count = int(
        node1.query(
            f"SELECT numbackends FROM test.stat WHERE datname = '{database_name}'"
        )
    )
    print(count)
    assert count <= 18  # 16 for test.test_table + 1 for conn + 1 for test.stat

    busy_pool = Pool(30)
    p = busy_pool.map_async(node_insert, range(30))
    p.wait()

    count = int(
        node1.query(
            f"SELECT numbackends FROM test.stat WHERE datname = '{database_name}'"
        )
    )
    print(count)
    assert count <= 19  # 16 for test.test_table + 1 for conn + at most 2 for test.stat

    busy_pool = Pool(30)
    p = busy_pool.map_async(node_insert_select, range(30))
    p.wait()

    count = int(
        node1.query(
            f"SELECT numbackends FROM test.stat WHERE datname = '{database_name}'"
        )
    )
    print(count)
    assert count <= 20  # 16 for test.test_table + 1 for conn + at most 3 for test.stat

    node1.query("DROP TABLE test.test_table;")
    node1.query("DROP TABLE test.stat;")


def test_datetime_with_timezone(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS test_timezone")
    node1.query("DROP TABLE IF EXISTS test.test_timezone")
    cursor.execute(
        "CREATE TABLE test_timezone (ts timestamp without time zone, ts_z timestamp with time zone)"
    )
    cursor.execute(
        "insert into test_timezone select '2014-04-04 20:00:00', '2014-04-04 20:00:00'::timestamptz at time zone 'America/New_York';"
    )
    cursor.execute("select * from test_timezone")
    result = cursor.fetchall()[0]
    logging.debug(f"{result[0]}, {str(result[1])[:-6]}")
    node1.query(
        "create table test.test_timezone ( ts DateTime, ts_z DateTime('America/New_York')) ENGINE PostgreSQL('postgres1:5432', 'postgres', 'test_timezone', 'postgres', 'mysecretpassword');"
    )
    assert node1.query("select ts from test.test_timezone").strip() == str(result[0])
    # [:-6] because 2014-04-04 16:00:00+00:00 -> 2014-04-04 16:00:00
    assert (
        node1.query("select ts_z from test.test_timezone").strip()
        == str(result[1])[:-6]
    )
    assert (
        node1.query("select * from test.test_timezone")
        == "2014-04-04 20:00:00\t2014-04-04 16:00:00\n"
    )
    cursor.execute("DROP TABLE test_timezone")
    node1.query("DROP TABLE test.test_timezone")


def test_postgres_ndim(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS arr1, arr2")

    cursor.execute("CREATE TABLE arr1 (a Integer[])")
    cursor.execute("INSERT INTO arr1 SELECT '{{1}, {2}}'")

    # The point is in creating a table via 'as select *', in postgres att_ndim will not be correct in this case.
    cursor.execute("CREATE TABLE arr2 AS SELECT * FROM arr1")
    cursor.execute(
        "SELECT attndims AS dims FROM pg_attribute WHERE attrelid = 'arr2'::regclass; "
    )
    result = cursor.fetchall()[0]
    assert int(result[0]) == 0

    result = node1.query(
        """SELECT toTypeName(a) FROM postgresql('postgres1:5432', 'postgres', 'arr2', 'postgres', 'mysecretpassword')"""
    )
    assert result.strip() == "Array(Array(Nullable(Int32)))"
    cursor.execute("DROP TABLE arr1, arr2")

    cursor.execute("DROP SCHEMA IF EXISTS ndim_schema CASCADE")
    cursor.execute("CREATE SCHEMA ndim_schema")
    cursor.execute("CREATE TABLE ndim_schema.arr1 (a integer[])")
    cursor.execute("INSERT INTO ndim_schema.arr1 SELECT '{{1}, {2}}'")
    # The point is in creating a table via 'as select *', in postgres att_ndim will not be correct in this case.
    cursor.execute("CREATE TABLE ndim_schema.arr2 AS SELECT * FROM ndim_schema.arr1")
    result = node1.query(
        """SELECT toTypeName(a) FROM postgresql(postgres1, schema='ndim_schema', table='arr2')"""
    )
    assert result.strip() == "Array(Array(Nullable(Int32)))"

    cursor.execute("DROP TABLE ndim_schema.arr1, ndim_schema.arr2")
    cursor.execute("DROP SCHEMA ndim_schema CASCADE")


def test_postgres_on_conflict(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    table = "test_conflict"
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.execute(f"CREATE TABLE {table} (a integer PRIMARY KEY, b text, c integer)")

    node1.query(
        """
        CREATE TABLE test.test_conflict (a UInt32, b String, c Int32)
        ENGINE PostgreSQL('postgres1:5432', 'postgres', 'test_conflict', 'postgres', 'mysecretpassword', '', 'ON CONFLICT DO NOTHING');
    """
    )
    node1.query(
        f""" INSERT INTO test.{table} SELECT number, concat('name_', toString(number)), 3 from numbers(100)"""
    )
    node1.query(
        f""" INSERT INTO test.{table} SELECT number, concat('name_', toString(number)), 4 from numbers(100)"""
    )

    check1 = f"SELECT count() FROM test.{table}"
    assert (node1.query(check1)).rstrip() == "100"

    table_func = f"""postgresql('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres', '{table}', 'postgres', 'mysecretpassword', '', 'ON CONFLICT DO NOTHING')"""
    node1.query(
        f"""INSERT INTO TABLE FUNCTION {table_func} SELECT number, concat('name_', toString(number)), 3 from numbers(100)"""
    )
    node1.query(
        f"""INSERT INTO TABLE FUNCTION {table_func} SELECT number, concat('name_', toString(number)), 3 from numbers(100)"""
    )

    check1 = f"SELECT count() FROM test.{table}"
    assert (node1.query(check1)).rstrip() == "100"

    cursor.execute(f"DROP TABLE {table} ")


def test_predefined_connection_configuration(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS test_table")
    cursor.execute(f"CREATE TABLE test_table (a integer PRIMARY KEY, b integer)")

    node1.query(
        """
        DROP TABLE IF EXISTS test.test_table;
        CREATE TABLE test.test_table (a UInt32, b Int32)
        ENGINE PostgreSQL(postgres1);
    """
    )
    node1.query(
        f""" INSERT INTO test.test_table SELECT number, number from numbers(100)"""
    )
    assert node1.query(f"SELECT count() FROM test.test_table").rstrip() == "100"

    node1.query(
        """
        DROP TABLE test.test_table;
        CREATE TABLE test.test_table (a UInt32, b Int32)
        ENGINE PostgreSQL(postgres1, on_conflict='ON CONFLICT DO NOTHING');
    """
    )
    node1.query(
        f""" INSERT INTO test.test_table SELECT number, number from numbers(100)"""
    )
    node1.query(
        f""" INSERT INTO test.test_table SELECT number, number from numbers(100)"""
    )
    assert node1.query(f"SELECT count() FROM test.test_table").rstrip() == "100"

    node1.query("DROP TABLE test.test_table;")
    node1.query_and_get_error(
        """
        CREATE TABLE test.test_table (a UInt32, b Int32)
        ENGINE PostgreSQL(postgres1, 'ON CONFLICT DO NOTHING');
    """
    )
    node1.query_and_get_error(
        """
        CREATE TABLE test.test_table (a UInt32, b Int32)
        ENGINE PostgreSQL(postgres2);
    """
    )
    node1.query_and_get_error(
        """
        CREATE TABLE test.test_table (a UInt32, b Int32)
        ENGINE PostgreSQL(unknown_collection);
    """
    )

    node1.query(
        """
        CREATE TABLE test.test_table (a UInt32, b Int32)
        ENGINE PostgreSQL(postgres1, port=5432, database='postgres', table='test_table');
    """
    )
    assert node1.query(f"SELECT count() FROM test.test_table").rstrip() == "100"

    node1.query(
        """
        DROP TABLE test.test_table;
        CREATE TABLE test.test_table (a UInt32, b Int32)
        ENGINE PostgreSQL(postgres3, port=5432);
    """
    )
    assert node1.query(f"SELECT count() FROM test.test_table").rstrip() == "100"

    assert node1.query(f"SELECT count() FROM postgresql(postgres1)").rstrip() == "100"
    node1.query(
        "INSERT INTO TABLE FUNCTION postgresql(postgres1, on_conflict='ON CONFLICT DO NOTHING') SELECT number, number from numbers(100)"
    )
    assert node1.query(f"SELECT count() FROM postgresql(postgres1)").rstrip() == "100"

    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")
    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.test_table (a integer)")
    node1.query(
        "INSERT INTO TABLE FUNCTION postgresql(postgres1, schema='test_schema', on_conflict='ON CONFLICT DO NOTHING') SELECT number from numbers(200)"
    )
    assert (
        node1.query(
            f"SELECT count() FROM postgresql(postgres1, schema='test_schema')"
        ).rstrip()
        == "200"
    )

    cursor.execute("DROP SCHEMA test_schema CASCADE")
    cursor.execute(f"DROP TABLE test_table ")


def test_where_false(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("CREATE TABLE test (a Integer)")
    cursor.execute("INSERT INTO test SELECT 1")

    result = node1.query(
        "SELECT count() FROM postgresql('postgres1:5432', 'postgres', 'test', 'postgres', 'mysecretpassword') WHERE 1=0"
    )
    assert int(result) == 0
    result = node1.query(
        "SELECT count() FROM postgresql('postgres1:5432', 'postgres', 'test', 'postgres', 'mysecretpassword') WHERE 0"
    )
    assert int(result) == 0
    result = node1.query(
        "SELECT count() FROM postgresql('postgres1:5432', 'postgres', 'test', 'postgres', 'mysecretpassword') WHERE 1=1"
    )
    assert int(result) == 1
    cursor.execute("DROP TABLE test")


def test_datetime64(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("drop table if exists test")
    cursor.execute("create table test (ts timestamp)")
    cursor.execute("insert into test select '1960-01-01 20:00:00';")

    result = node1.query("select * from postgresql(postgres1, table='test')")
    assert result.strip() == "1960-01-01 20:00:00.000000"


def test_uuid(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("drop table if exists test")
    cursor.execute("create table test (u uuid)")
    cursor.execute("""CREATE EXTENSION IF NOT EXISTS "uuid-ossp";""")
    cursor.execute("insert into test select uuid_generate_v1();")

    result = node1.query(
        "select toTypeName(u) from postgresql(postgres1, table='test')"
    )
    assert result.strip() == "Nullable(UUID)"


def test_auto_close_connection(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=False
    )
    cursor = conn.cursor()
    database_name = "auto_close_connection_test"

    cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cursor.execute(f"CREATE DATABASE {database_name}")
    conn = get_postgres_conn(
        started_cluster.postgres_ip,
        started_cluster.postgres_port,
        database=True,
        database_name=database_name,
    )
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test_table (key integer, value integer)")

    node2.query(
        f"""
        CREATE TABLE test.test_table (key UInt32, value UInt32)
        ENGINE = PostgreSQL(postgres1, database='{database_name}', table='test_table')
    """
    )

    result = node2.query(
        "INSERT INTO test.test_table SELECT number, number FROM numbers(1000)",
        user="default",
    )

    result = node2.query("SELECT * FROM test.test_table LIMIT 100", user="default")

    node2.query(
        f"""
        CREATE TABLE test.stat (numbackends UInt32, datname String)
        ENGINE = PostgreSQL(postgres1, database='{database_name}', table='pg_stat_database')
    """
    )

    count = int(
        node2.query(
            f"SELECT numbackends FROM test.stat WHERE datname = '{database_name}'"
        )
    )

    # Connection from python + pg_stat table also has a connection at the moment of current query
    assert count == 2

    node2.query("DROP TABLE test.stat")
    node2.query("DROP TABLE test.test_table")


def test_literal_escaping(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS escaping")
    cursor.execute(f"CREATE TABLE escaping(text varchar(255))")
    node1.query(
        "CREATE TABLE default.escaping (text String) ENGINE = PostgreSQL('postgres1:5432', 'postgres', 'escaping', 'postgres', 'mysecretpassword')"
    )
    node1.query("SELECT * FROM escaping WHERE text = ''''")  # ' -> ''
    node1.query("SELECT * FROM escaping WHERE text = '\\''")  # ' -> ''
    node1.query("SELECT * FROM escaping WHERE text = '\\\\\\''")  # \' -> \''
    node1.query("SELECT * FROM escaping WHERE text = '\\\\\\''")  # \' -> \''
    node1.query("SELECT * FROM escaping WHERE text like '%a''a%'")  # %a'a% -> %a''a%
    node1.query("SELECT * FROM escaping WHERE text like '%a\\'a%'")  # %a'a% -> %a''a%
    cursor.execute(f"DROP TABLE escaping")
    node1.query("DROP TABLE default.escaping")


def test_filter_pushdown(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("CREATE SCHEMA test_filter_pushdown")
    cursor.execute(
        "CREATE TABLE test_filter_pushdown.test_table (id integer, value integer)"
    )
    cursor.execute(
        "INSERT INTO test_filter_pushdown.test_table VALUES (1, 10), (1, 110), (2, 0), (3, 33), (4, 0)"
    )

    node1.query("DROP TABLE IF EXISTS test_filter_pushdown_pg_table")
    node1.query(
        """
        CREATE TABLE test_filter_pushdown_pg_table (id UInt32, value UInt32)
        ENGINE PostgreSQL('postgres1:5432', 'postgres', 'test_table', 'postgres', 'mysecretpassword', 'test_filter_pushdown');
    """
    )

    node1.query("DROP TABLE IF EXISTS test_filter_pushdown_local_table")
    node1.query(
        """
        CREATE TABLE test_filter_pushdown_local_table (id UInt32, value UInt32) ENGINE Memory AS SELECT * FROM test_filter_pushdown_pg_table
    """
    )

    node1.query("DROP TABLE IF EXISTS ch_table")
    node1.query(
        "CREATE TABLE ch_table (id UInt32, pg_id UInt32) ENGINE MergeTree ORDER BY id"
    )
    node1.query("INSERT INTO ch_table VALUES (1, 1), (2, 2), (3, 1), (4, 2), (5, 999)")

    def compare_results(query, **kwargs):
        result1 = node1.query(
            query.format(pg_table="test_filter_pushdown_pg_table", **kwargs)
        )
        result2 = node1.query(
            query.format(pg_table="test_filter_pushdown_local_table", **kwargs)
        )
        assert result1 == result2

    for kind in ["INNER", "LEFT", "RIGHT", "FULL", "ANY LEFT", "SEMI RIGHT"]:
        for value in [0, 10]:
            compare_results(
                "SELECT * FROM ch_table {kind} JOIN {pg_table} as p ON ch_table.pg_id = p.id WHERE value = {value} ORDER BY ALL",
                kind=kind,
                value=value,
            )

            compare_results(
                "SELECT * FROM {pg_table} as p {kind} JOIN ch_table ON ch_table.pg_id = p.id WHERE value = {value} ORDER BY ALL",
                kind=kind,
                value=value,
            )

    cursor.execute("DROP SCHEMA test_filter_pushdown CASCADE")
    node1.query("DROP TABLE test_filter_pushdown_local_table")
    node1.query("DROP TABLE test_filter_pushdown_pg_table")


def test_fixed_string_type(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS test_fixed_string")
    cursor.execute(
        "CREATE TABLE test_fixed_string (contact_id numeric NULL, email varchar NULL)"
    )
    cursor.execute("INSERT INTO test_fixed_string values (1, 'abc')")

    node1.query("DROP TABLE IF EXISTS test_fixed_string")
    node1.query(
        "CREATE TABLE test_fixed_string(contact_id Int64, email Nullable(FixedString(3))) ENGINE = PostgreSQL('postgres1:5432', 'postgres', 'test_fixed_string', 'postgres', 'mysecretpassword')"
    )

    result = node1.query("SELECT * FROM test_fixed_string format TSV")

    assert result.strip() == "1\tabc"

    node1.query("DROP TABLE test_fixed_string")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()

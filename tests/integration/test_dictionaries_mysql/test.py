## sudo -H pip install PyMySQL
from contextlib import contextmanager
import logging
import socket
import threading
import time
import warnings

import pymysql.cursors
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.port_forward import PortForward
from helpers.config_cluster import mysql_pass

DICTS = [
    "configs/dictionaries/mysql_dict1.xml",
    "configs/dictionaries/mysql_dict2.xml",
    "configs/dictionaries/mysql_dict_compression.xml",
    "configs/dictionaries/mysql_dict_compression_wire.xml",
    "configs/dictionaries/mysql_dict_no_compression_wire.xml",
]
CONFIG_FILES = [
    "configs/remote_servers.xml",
    "configs/named_collections.xml",
    "configs/bg_reconnect.xml",
]
USER_CONFIGS = ["configs/users.xml"]
cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=CONFIG_FILES,
    user_configs=USER_CONFIGS,
    with_mysql8=True,
    dictionaries=DICTS,
)

create_table_mysql_template = """
    CREATE TABLE IF NOT EXISTS `test`.`{}` (
        `id` int(11) NOT NULL,
        `value` varchar(50) NOT NULL,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB;
    """

create_clickhouse_dictionary_table_template = """
    CREATE TABLE IF NOT EXISTS `test`.`dict_table_{}` (`id` UInt64, `value` String) ENGINE = Dictionary({})
    """


@pytest.fixture(scope="module")
def started_cluster():
    try:
        # time.sleep(30)
        cluster.start()

        # Create a MySQL database
        mysql_connection = get_mysql_conn(cluster)
        create_mysql_db(mysql_connection, "test")
        mysql_connection.close()

        # Create database in ClickHouse
        instance.query("DROP DATABASE IF EXISTS test")
        instance.query("CREATE DATABASE test")

        # Create database in ClickChouse using MySQL protocol (will be used for data insertion)
        instance.query(
            f"CREATE DATABASE clickhouse_mysql ENGINE = MySQL('mysql80:3306', 'test', 'root', '{mysql_pass}')"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_mysql_dictionaries_custom_query_full_load(started_cluster):
    mysql_connection = get_mysql_conn(started_cluster)

    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_table_1 (id Integer, value_1 Text);",
    )
    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_table_2 (id Integer, value_2 Text);",
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.test_table_1 VALUES (1, 'Value_1');"
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.test_table_2 VALUES (1, 'Value_2');"
    )

    query = instance.query
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
    SOURCE(MYSQL(
        HOST 'mysql80'
        PORT 3306
        USER 'root'
        PASSWORD '{mysql_pass}'
        QUERY $doc$SELECT id, value_1, value_2 FROM test.test_table_1 INNER JOIN test.test_table_2 USING (id);$doc$))
    LIFETIME(0)
    """
    )

    result = query(
        "SELECT dictGetString('test_dictionary_custom_query', 'value_1', toUInt64(1))"
    )
    assert result == "Value_1\n"

    result = query("SELECT id, value_1, value_2 FROM test_dictionary_custom_query")
    assert result == "1\tValue_1\tValue_2\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

    query(
        f"""
    CREATE DICTIONARY test_cache_dictionary_custom_query
    (
        id1 UInt64,
        id2 UInt64,
        value_concat String
    )
    PRIMARY KEY id1, id2
    LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 10))
    SOURCE(MYSQL(
        HOST 'mysql80'
        PORT 3306
        USER 'root'
        PASSWORD '{mysql_pass}'
        QUERY 'SELECT id AS id1, id + 1 AS id2, CONCAT_WS(" ", "The", value_1) AS value_concat FROM test.test_table_1'))
    LIFETIME(0)
    """
    )

    result = query(
        "SELECT dictGetString('test_cache_dictionary_custom_query', 'value_concat', (1, 2))"
    )
    assert result == "The Value_1\n"

    result = query("SELECT id1, value_concat FROM test_cache_dictionary_custom_query")
    assert result == "1\tThe Value_1\n"

    query("DROP DICTIONARY test_cache_dictionary_custom_query;")

    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_1;")
    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_2;")


def test_mysql_dictionaries_custom_query_partial_load_simple_key(started_cluster):
    mysql_connection = get_mysql_conn(started_cluster)

    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_table_1 (id Integer, value_1 Text);",
    )
    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_table_2 (id Integer, value_2 Text);",
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.test_table_1 VALUES (1, 'Value_1');"
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.test_table_2 VALUES (1, 'Value_2');"
    )

    query = instance.query
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
    SOURCE(MYSQL(
        HOST 'mysql80'
        PORT 3306
        USER 'root'
        PASSWORD '{mysql_pass}'
        QUERY $doc$SELECT id, value_1, value_2 FROM test.test_table_1 INNER JOIN test.test_table_2 USING (id) WHERE {{condition}};$doc$))
    """
    )

    result = query(
        "SELECT dictGet('test_dictionary_custom_query', ('value_1', 'value_2'), toUInt64(1))"
    )

    assert result == "('Value_1','Value_2')\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_1;")
    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_2;")


def test_mysql_dictionaries_custom_query_partial_load_complex_key(started_cluster):
    mysql_connection = get_mysql_conn(started_cluster)

    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_table_1 (id Integer, id_key Text, value_1 Text);",
    )
    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_table_2 (id Integer, id_key Text, value_2 Text);",
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.test_table_1 VALUES (1, 'Key', 'Value_1');"
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.test_table_2 VALUES (1, 'Key', 'Value_2');"
    )

    query = instance.query
    query(
        f"""
    CREATE DICTIONARY test_dictionary_custom_query
    (
        id UInt64,
        id_key String,
        value_1 String,
        value_2 String
    )
    PRIMARY KEY id, id_key
    LAYOUT(COMPLEX_KEY_DIRECT())
    SOURCE(MYSQL(
        HOST 'mysql80'
        PORT 3306
        USER 'root'
        PASSWORD '{mysql_pass}'
        QUERY $doc$SELECT id, id_key, value_1, value_2 FROM test.test_table_1 INNER JOIN test.test_table_2 USING (id, id_key) WHERE {{condition}};$doc$))
    """
    )

    result = query(
        "SELECT dictGet('test_dictionary_custom_query', ('value_1', 'value_2'), (toUInt64(1), 'Key'))"
    )

    assert result == "('Value_1','Value_2')\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_1;")
    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_2;")


def test_mysql_dict_complex_key_with_special_chars(started_cluster):
    """Regression test: ExternalQueryBuilder uses backslash escaping for MySQL backend.

    MySQL uses IdentifierQuotingStyle::Backticks, so escape_quote_with_quote stays false
    (backslash escaping: ' -> \\', \\ -> \\\\). Verify that keys containing single quotes
    and backslashes are looked up correctly via dictGet.
    """
    mysql_connection = get_mysql_conn(started_cluster)

    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_mysql_escape (key_col TEXT, value_col TEXT);",
    )
    # Single-quote key: use double-quote MySQL string literal to avoid escaping.
    execute_mysql_query(
        mysql_connection,
        "INSERT INTO test.test_mysql_escape VALUES (\"it's\", 'quote');",
    )
    # Backslash key: use CHAR(92) to insert a literal backslash without SQL escaping confusion.
    execute_mysql_query(
        mysql_connection,
        "INSERT INTO test.test_mysql_escape VALUES (CONCAT('foo', CHAR(92), 'bar'), 'backslash');",
    )
    execute_mysql_query(
        mysql_connection,
        "INSERT INTO test.test_mysql_escape VALUES ('normal', 'normal value');",
    )

    query = instance.query
    query(
        f"""
    CREATE DICTIONARY test_dict_mysql_escape
    (
        key_col String,
        value_col String DEFAULT ''
    )
    PRIMARY KEY key_col
    LAYOUT(COMPLEX_KEY_DIRECT())
    SOURCE(MYSQL(
        HOST 'mysql80'
        PORT 3306
        USER 'root'
        PASSWORD '{mysql_pass}'
        DB 'test'
        TABLE 'test_mysql_escape'))
    """
    )

    result = query(
        "SELECT dictGet('test_dict_mysql_escape', 'value_col', tuple('it\\'s'))"
    )
    assert result == "quote\n", f"Unexpected result: {result!r}"

    result = query(
        "SELECT dictGet('test_dict_mysql_escape', 'value_col', tuple('foo\\\\bar'))"
    )
    assert result == "backslash\n", f"Unexpected result: {result!r}"

    result = query(
        "SELECT dictGet('test_dict_mysql_escape', 'value_col', tuple('normal'))"
    )
    assert result == "normal value\n", f"Unexpected result: {result!r}"

    result = query(
        "SELECT dictGet('test_dict_mysql_escape', 'value_col', tuple('missing'))"
    )
    assert result == "\n", f"Unexpected result: {result!r}"

    query("DROP DICTIONARY test_dict_mysql_escape;")
    execute_mysql_query(mysql_connection, "DROP TABLE test.test_mysql_escape;")


def test_predefined_connection_configuration(started_cluster):
    mysql_connection = get_mysql_conn(started_cluster)

    execute_mysql_query(mysql_connection, "DROP TABLE IF EXISTS test.test_table")
    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE IF NOT EXISTS test.test_table (id Integer, value Integer);",
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.test_table VALUES (100, 200);"
    )

    instance.query(
        """
    DROP DICTIONARY IF EXISTS dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME mysql1))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query("SELECT dictGetUInt32(dict, 'value', toUInt64(100))")
    assert int(result) == 200

    instance.query(
        """
    DROP DICTIONARY dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME mysql2))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query_and_get_error(
        "SELECT dictGetUInt32(dict, 'value', toUInt64(100))"
    )
    instance.query(
        """
    DROP DICTIONARY dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME unknown_collection))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query_and_get_error(
        "SELECT dictGetUInt32(dict, 'value', toUInt64(100))"
    )

    instance.query(
        """
    DROP DICTIONARY dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME mysql3 PORT 3306))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query("SELECT dictGetUInt32(dict, 'value', toUInt64(100))")
    assert int(result) == 200

    instance.query(
        """
    DROP DICTIONARY IF EXISTS dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME mysql1 connection_pool_size 0))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query_and_get_error(
        "SELECT dictGetUInt32(dict, 'value', toUInt64(100))"
    )
    assert "Connection pool cannot have zero size" in result

    instance.query(
        """
    DROP DICTIONARY IF EXISTS dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME mysql4))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query_and_get_error(
        "SELECT dictGetUInt32(dict, 'value', toUInt64(100))"
    )
    assert "Connection pool cannot have zero size" in result

    instance.query(
        """
    DROP DICTIONARY IF EXISTS dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME mysql4 connection_pool_size 1))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query("SELECT dictGetUInt32(dict, 'value', toUInt64(100))")
    assert int(result) == 200

    instance.query(
        """
    DROP DICTIONARY IF EXISTS dict;
    CREATE DICTIONARY dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(MYSQL(NAME mysql4 connection_pool_size 1 close_connection 1 share_connection 1))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    """
    )
    result = instance.query("SELECT dictGetUInt32(dict, 'value', toUInt64(100))")
    assert int(result) == 200


def create_mysql_db(mysql_connection, name):
    with mysql_connection.cursor() as cursor:
        cursor.execute("DROP DATABASE IF EXISTS {}".format(name))
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))


def prepare_mysql_table(started_cluster, table_name, index):
    mysql_connection = get_mysql_conn(started_cluster)

    # Create table
    create_mysql_table(mysql_connection, table_name + str(index))

    # Insert rows using CH
    query = instance.query
    query(
        "INSERT INTO `clickhouse_mysql`.{}(id, value) select number, concat('{} value ', toString(number)) from numbers(10000) ".format(
            table_name + str(index), table_name + str(index)
        )
    )
    assert (
        query(
            "SELECT count() FROM `clickhouse_mysql`.{}".format(table_name + str(index))
        ).rstrip()
        == "10000"
    )
    mysql_connection.close()

    # Create CH Dictionary tables based on MySQL tables
    query(
        create_clickhouse_dictionary_table_template.format(
            table_name + str(index), "dict" + str(index)
        )
    )


def get_mysql_conn(started_cluster):
    errors = []
    conn = None
    for _ in range(5):
        try:
            if conn is None:
                conn = pymysql.connect(
                    user="root",
                    password=mysql_pass,
                    host=started_cluster.mysql8_ip,
                    port=started_cluster.mysql8_port,
                )
            else:
                conn.ping(reconnect=True)
            logging.debug(
                f"MySQL Connection establised: {started_cluster.mysql8_ip}:{started_cluster.mysql8_port}"
            )
            return conn
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)

    raise Exception("Connection not establised, {}".format(errors))


def execute_mysql_query(connection, query):
    logging.debug("Execute MySQL query:{}".format(query))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()


def create_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(create_table_mysql_template.format(table_name))


def test_background_dictionary_reconnect(started_cluster):
    mysql_connection = get_mysql_conn(started_cluster)

    execute_mysql_query(mysql_connection, "DROP TABLE IF EXISTS test.dict;")
    execute_mysql_query(
        mysql_connection,
        "CREATE TABLE test.dict (id Integer, value Text);",
    )
    execute_mysql_query(
        mysql_connection, "INSERT INTO test.dict VALUES (1, 'Value_1');"
    )

    port_forward = PortForward()
    port = port_forward.start((started_cluster.mysql8_ip, started_cluster.mysql8_port))

    @contextmanager
    def port_forward_manager():
        try:
            yield
        finally:
            port_forward.stop(True)

    with port_forward_manager():

        query = instance.query
        query(
            f"""
        DROP DICTIONARY IF EXISTS dict;
        CREATE DICTIONARY dict
        (
            id UInt64,
            value String
        )
        PRIMARY KEY id
        LAYOUT(DIRECT())
        SOURCE(MYSQL(
            USER 'root'
            PASSWORD '{mysql_pass}'
            DB 'test'
            QUERY $doc$SELECT * FROM test.dict;$doc$
            BACKGROUND_RECONNECT 'true'
            REPLICA(HOST '{socket.gethostbyname(socket.gethostname())}' PORT {port} PRIORITY 1)))
        """
        )

        result = query("SELECT value FROM dict WHERE id = 1")
        assert result == "Value_1\n"

        class MySQL_Instance:
            pass

        mysql_instance = MySQL_Instance()
        mysql_instance.ip_address = started_cluster.mysql8_ip

        query("TRUNCATE TABLE IF EXISTS system.text_log")

        # Break connection to mysql server
        port_forward.stop(force=True)

        # Exhaust possible connection pool and initiate reconnection attempts
        for _ in range(5):
            try:
                result = query("SELECT value FROM dict WHERE id = 1")
            except Exception:
                pass

        time.sleep(5)

        # Restore connection to mysql server
        port_forward.start((started_cluster.mysql8_ip, started_cluster.mysql8_port), port)

        time.sleep(5)

        query("SYSTEM FLUSH LOGS")
        assert (
            int(
                query(
                    "SELECT count() FROM system.text_log WHERE message like 'Failed to connect to MySQL %: mysqlxx::ConnectionFailed'"
                )
            )
            > 0
        )
        assert (
            int(
                query(
                    "SELECT count() FROM system.text_log WHERE message like 'Reestablishing connection to % has succeeded.'"
                )
            )
            > 0
        )
        assert (
            int(
                query(
                    "SELECT count() FROM system.text_log WHERE message like '%Reestablishing connection to % has failed: mysqlxx::ConnectionFailed: Can\\'t connect to server on %'"
                )
            )
            > 0
        )


def test_enable_compression_xml_dict(started_cluster):
    """Regression: <enable_compression>1</enable_compression> in XML dictionary source must not be rejected."""
    mysql_connection = get_mysql_conn(started_cluster)

    try:
        execute_mysql_query(mysql_connection, "DROP TABLE IF EXISTS test.dict_compression_table;")
        execute_mysql_query(
            mysql_connection,
            "CREATE TABLE test.dict_compression_table (id INT NOT NULL, value TEXT, PRIMARY KEY(id));",
        )
        execute_mysql_query(
            mysql_connection,
            "INSERT INTO test.dict_compression_table VALUES (1, 'compressed');",
        )

        # Regression: verify that <enable_compression>1</enable_compression> in the XML
        # source config is accepted by the parser and does not raise UNKNOWN_SETTING.
        # The dict may be in FAILED state here (dict_compression_table did not exist at
        # server startup so the initial load failed), but any failure must be a connection
        # or table error — not a configuration-rejection error.
        last_exception = instance.query(
            "SELECT last_exception FROM system.dictionaries WHERE name = 'dict_compression'"
        ).strip()
        assert "UNKNOWN_SETTING" not in last_exception, (
            f"<enable_compression> was rejected in the XML dict config: {last_exception!r}"
        )

        # Pool-isolation regression: dict_compression_wire (enable_compression=1,
        # share_connection=1) and dict_no_compression_wire (enable_compression=0,
        # share_connection=1) share the same host/user/db/port. The PoolFactory cache
        # key must include the compression flag so each dict gets its own pool.
        # Reload both in one cycle so they compete for the same cache lifetime; a broken
        # discriminator would cause them to share a pool and one assertion below would fail.
        for _ in range(10):
            try:
                instance.query("SYSTEM RELOAD DICTIONARIES")
                break
            except Exception:
                time.sleep(0.5)

        xml_wire_compression = ""
        for _ in range(10):
            xml_wire_compression = instance.query(
                "SELECT dictGet('dict_compression_wire', 'VARIABLE_VALUE', 'Compression')"
            ).strip()
            if xml_wire_compression == "ON":
                break
            time.sleep(0.5)
        assert xml_wire_compression == "ON", (
            f"Expected Compression=ON via XML dict config path, got: {xml_wire_compression!r}"
        )

        no_compression_status = ""
        for _ in range(10):
            no_compression_status = instance.query(
                "SELECT dictGet('dict_no_compression_wire', 'VARIABLE_VALUE', 'Compression')"
            ).strip()
            if no_compression_status == "OFF":
                break
            time.sleep(0.5)
        assert no_compression_status == "OFF", (
            f"Expected Compression=OFF for uncompressed dict, got: {no_compression_status!r}"
        )

        # Wire-level assertion for the DDL dictionary source path (address-based
        # PoolWithFailover constructor via createMySQLPoolWithFailover). This is a separate
        # code path from the XML config pool above.
        instance.query("DROP DICTIONARY IF EXISTS dict_compression_wire_check")
        instance.query(
            f"""
            CREATE DICTIONARY dict_compression_wire_check
            (
                VARIABLE_NAME String,
                VARIABLE_VALUE String
            )
            PRIMARY KEY VARIABLE_NAME
            SOURCE(MYSQL(
                host 'mysql80'
                port 3306
                user 'root'
                password '{mysql_pass}'
                db 'performance_schema'
                table 'session_status'
                enable_compression 1
            ))
            LAYOUT(COMPLEX_KEY_HASHED())
            LIFETIME(0)
            """
        )
        try:
            wire_compression = ""
            for _ in range(10):
                wire_compression = instance.query(
                    "SELECT dictGet('dict_compression_wire_check', 'VARIABLE_VALUE', 'Compression')"
                ).strip()
                if wire_compression == "ON":
                    break
                time.sleep(0.5)
            assert wire_compression == "ON", (
                f"Expected Compression=ON via DDL dict source path, got: {wire_compression!r}"
            )
        finally:
            instance.query("DROP DICTIONARY IF EXISTS dict_compression_wire_check")
    finally:
        execute_mysql_query(mysql_connection, "DROP TABLE IF EXISTS test.dict_compression_table;")
        mysql_connection.close()


def test_mysql_shared_connection_pool_limit(started_cluster):
    """Regression for https://github.com/ClickHouse/ClickHouse/issues/22048

    XML / config-path dictionaries (i.e. without a named collection) must honor
    `connection_pool_size` and `connection_wait_timeout`. Previously these were ignored on
    that path, and a shared pool (`share_connection=true`) always waited indefinitely for a
    free connection (wait_timeout = UINT64_MAX). As a result, concurrent loads through a
    single shared pool could freeze SYSTEM RELOAD DICTIONAR{Y,IES} once all connections were
    in use, instead of failing with a clear error.

    Two dictionaries that resolve to the same shared, single-connection pool are reloaded
    concurrently. With the fix, the reload that loses the race for the only connection must
    fail fast (`connection_wait_timeout 0` => "do not wait"). Before the fix the size limit
    was ignored, the pool had 16 connections, and both reloads simply succeeded.
    """
    mysql_connection = get_mysql_conn(started_cluster)

    def make_dict(name):
        instance.query(f"DROP DICTIONARY IF EXISTS {name}")
        instance.query(
            f"""
            CREATE DICTIONARY {name} (id UInt32, value UInt32)
            PRIMARY KEY id
            SOURCE(MYSQL(
                host 'mysql80'
                port 3306
                user 'root'
                password '{mysql_pass}'
                db 'test'
                query 'SELECT id, value FROM test.shared_pool_test WHERE SLEEP(5) = 0'
                share_connection 1
                close_connection 1
                connection_pool_size 1
                connection_wait_timeout 0
            ))
            LAYOUT(HASHED())
            LIFETIME(0)
            """
        )

    try:
        execute_mysql_query(
            mysql_connection, "DROP TABLE IF EXISTS test.shared_pool_test;"
        )
        execute_mysql_query(
            mysql_connection,
            "CREATE TABLE test.shared_pool_test (id INT NOT NULL, value INT NOT NULL, PRIMARY KEY(id));",
        )
        execute_mysql_query(
            mysql_connection, "INSERT INTO test.shared_pool_test VALUES (1, 100);"
        )

        make_dict("shared_pool_dict_a")
        make_dict("shared_pool_dict_b")

        # Reload both dictionaries concurrently. They resolve to the same shared pool
        # (identical host/port/user/db) which now holds a single connection. One reload
        # acquires it and runs the slow query; the other must fail fast instead of waiting
        # forever (the freeze) or grabbing an out-of-limit connection (the pre-fix behavior).
        results = {}

        def reload(name):
            try:
                instance.query(f"SYSTEM RELOAD DICTIONARY {name}")
                results[name] = None
            except Exception as e:  # noqa: BLE001
                results[name] = str(e)

        threads = [
            threading.Thread(target=reload, args=("shared_pool_dict_a",)),
            threading.Thread(target=reload, args=("shared_pool_dict_b",)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        for t in threads:
            assert (
                not t.is_alive()
            ), "SYSTEM RELOAD DICTIONARY hung on a shared connection pool (issue #22048)"

        errors = [msg for msg in results.values() if msg]
        successes = [name for name, msg in results.items() if not msg]

        assert len(errors) >= 1, (
            "Expected the shared single-connection pool to reject the second concurrent "
            f"reload, but both succeeded (connection_pool_size was ignored): {results}"
        )
        assert any("Pool is full" in msg for msg in errors), (
            f"Expected a 'Pool is full' error from the exhausted shared pool, got: {errors}"
        )
        assert len(successes) >= 1, f"Expected exactly one reload to succeed: {results}"

        # The winning concurrent reload returns its single shared connection to the pool only
        # after the client has already received the query result, so there is a brief window
        # in which the connection is not recycled yet. Because these dictionaries use
        # `connection_wait_timeout 0` (fail fast, never wait), a reload issued inside that
        # window can still observe "Pool is full". Retry until the connection becomes available
        # again (it always does shortly after the slow reload finishes); any other error is a
        # real failure.
        reloaded = False
        for _ in range(60):
            try:
                instance.query("SYSTEM RELOAD DICTIONARY shared_pool_dict_a")
                reloaded = True
                break
            except Exception as e:  # noqa: BLE001
                assert "Pool is full" in str(e), f"Unexpected reload error: {e}"
                time.sleep(0.5)
        assert reloaded, "Shared connection pool never freed up after concurrent reloads"
        value = instance.query(
            "SELECT dictGetUInt32('shared_pool_dict_a', 'value', toUInt64(1))"
        ).strip()
        assert value == "100", f"Unexpected dictionary value: {value!r}"
    finally:
        instance.query("DROP DICTIONARY IF EXISTS shared_pool_dict_a")
        instance.query("DROP DICTIONARY IF EXISTS shared_pool_dict_b")
        execute_mysql_query(
            mysql_connection, "DROP TABLE IF EXISTS test.shared_pool_test;"
        )
        mysql_connection.close()


def test_mysql_config_pool_zero_size_rejected(started_cluster):
    """A config-path (XML / DDL without a named collection) MySQL dictionary must reject
    `connection_pool_size 0`, matching `createMySQLPoolWithFailover` (the named-collection / DDL
    path). Otherwise `max_connections` becomes 0 while the pool still hands out the default start
    connection (it is allocated before `max_connections` is enforced), so the pool is neither
    rejected nor truly zero-sized.
    """
    mysql_connection = get_mysql_conn(started_cluster)
    try:
        execute_mysql_query(
            mysql_connection, "DROP TABLE IF EXISTS test.zero_pool_test;"
        )
        execute_mysql_query(
            mysql_connection,
            "CREATE TABLE test.zero_pool_test (id INT NOT NULL, value INT NOT NULL, PRIMARY KEY(id));",
        )
        execute_mysql_query(
            mysql_connection, "INSERT INTO test.zero_pool_test VALUES (1, 100);"
        )

        instance.query("DROP DICTIONARY IF EXISTS zero_pool_dict")
        instance.query(
            f"""
            CREATE DICTIONARY zero_pool_dict (id UInt32, value UInt32)
            PRIMARY KEY id
            SOURCE(MYSQL(
                host 'mysql80'
                port 3306
                user 'root'
                password '{mysql_pass}'
                db 'test'
                query 'SELECT id, value FROM test.zero_pool_test'
                connection_pool_size 0
            ))
            LAYOUT(HASHED())
            LIFETIME(0)
            """
        )

        error = instance.query_and_get_error("SYSTEM RELOAD DICTIONARY zero_pool_dict")
        assert "Connection pool cannot have zero size" in error, (
            f"Expected a zero-size pool to be rejected on the config path, got: {error!r}"
        )
    finally:
        instance.query("DROP DICTIONARY IF EXISTS zero_pool_dict")
        execute_mysql_query(
            mysql_connection, "DROP TABLE IF EXISTS test.zero_pool_test;"
        )
        mysql_connection.close()


def test_mysql_shared_pool_no_setting_inheritance(started_cluster):
    """Regression for the shared-pool setting-inheritance bug (review of #108083 / issue #22048).

    The PoolFactory cache must key shared pools by their settings (`connection_pool_size`,
    `connection_wait_timeout`) too, not only by connection identity (host/port/user/db/compression).
    Otherwise the dictionary that creates the cached pool first wins: a default-settings dictionary
    (16 connections, 5 second wait) seeds the cache, and later dictionaries with the same endpoint
    but `connection_pool_size 1` silently inherit that 16-connection pool instead of getting their
    own bounded one.

    A default-settings dictionary is loaded first via a plain `dictGet` to seed the pool cache for
    the endpoint (a lazy load does not reset the cache, unlike SYSTEM RELOAD DICTIONAR{Y,IES}). Two
    `connection_pool_size 1, connection_wait_timeout 0` dictionaries for the same endpoint are then
    reloaded concurrently. With the fix they share their own single-connection pool, so the reload
    that loses the race for the only connection fails fast with "Pool is full". Before the fix both
    inherited the cached default 16-connection pool and both reloads succeeded.
    """
    mysql_connection = get_mysql_conn(started_cluster)

    def make_limited_dict(name):
        instance.query(f"DROP DICTIONARY IF EXISTS {name}")
        instance.query(
            f"""
            CREATE DICTIONARY {name} (id UInt32, value UInt32)
            PRIMARY KEY id
            SOURCE(MYSQL(
                host 'mysql80'
                port 3306
                user 'root'
                password '{mysql_pass}'
                db 'test'
                query 'SELECT id, value FROM test.inherit_pool_test WHERE SLEEP(5) = 0'
                share_connection 1
                close_connection 1
                connection_pool_size 1
                connection_wait_timeout 0
            ))
            LAYOUT(HASHED())
            LIFETIME(0)
            """
        )

    try:
        execute_mysql_query(
            mysql_connection, "DROP TABLE IF EXISTS test.inherit_pool_test;"
        )
        execute_mysql_query(
            mysql_connection,
            "CREATE TABLE test.inherit_pool_test (id INT NOT NULL, value INT NOT NULL, PRIMARY KEY(id));",
        )
        execute_mysql_query(
            mysql_connection, "INSERT INTO test.inherit_pool_test VALUES (1, 100);"
        )

        # Default-settings dictionary for the same endpoint: shared pool, default size (16) and
        # default wait timeout (5s), fast query. Loading it first seeds the PoolFactory cache.
        instance.query("DROP DICTIONARY IF EXISTS inherit_default_dict")
        instance.query(
            f"""
            CREATE DICTIONARY inherit_default_dict (id UInt32, value UInt32)
            PRIMARY KEY id
            SOURCE(MYSQL(
                host 'mysql80'
                port 3306
                user 'root'
                password '{mysql_pass}'
                db 'test'
                query 'SELECT id, value FROM test.inherit_pool_test'
                share_connection 1
                close_connection 1
            ))
            LAYOUT(HASHED())
            LIFETIME(0)
            """
        )

        make_limited_dict("inherit_limited_dict_a")
        make_limited_dict("inherit_limited_dict_b")

        # Lazy-load the default dictionary to populate the shared-pool cache for the endpoint
        # without resetting it (a plain dictGet does not call PoolFactory::reset, unlike a reload).
        value = instance.query(
            "SELECT dictGetUInt32('inherit_default_dict', 'value', toUInt64(1))"
        ).strip()
        assert value == "100", f"Unexpected default dictionary value: {value!r}"

        # Concurrently reload the two limited dictionaries. With the fix they get their own
        # single-connection pool (keyed by their settings), distinct from the cached default
        # 16-connection pool, so the reload that loses the race for the only connection fails fast.
        results = {}

        def reload(name):
            try:
                instance.query(f"SYSTEM RELOAD DICTIONARY {name}")
                results[name] = None
            except Exception as e:  # noqa: BLE001
                results[name] = str(e)

        threads = [
            threading.Thread(target=reload, args=("inherit_limited_dict_a",)),
            threading.Thread(target=reload, args=("inherit_limited_dict_b",)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        for t in threads:
            assert (
                not t.is_alive()
            ), "SYSTEM RELOAD DICTIONARY hung on a shared connection pool (issue #22048)"

        errors = [msg for msg in results.values() if msg]
        assert any("Pool is full" in msg for msg in errors), (
            "Expected the limited dictionaries to use their own single-connection pool and reject "
            f"the second concurrent reload, but they inherited the cached default pool: {results}"
        )
    finally:
        instance.query("DROP DICTIONARY IF EXISTS inherit_default_dict")
        instance.query("DROP DICTIONARY IF EXISTS inherit_limited_dict_a")
        instance.query("DROP DICTIONARY IF EXISTS inherit_limited_dict_b")
        execute_mysql_query(
            mysql_connection, "DROP TABLE IF EXISTS test.inherit_pool_test;"
        )
        mysql_connection.close()

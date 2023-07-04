## sudo -H pip install PyMySQL
import warnings
import pymysql.cursors
import pytest
from helpers.cluster import ClickHouseCluster
import time
import logging

DICTS = ["configs/dictionaries/mysql_dict1.xml", "configs/dictionaries/mysql_dict2.xml"]
CONFIG_FILES = ["configs/remote_servers.xml", "configs/named_collections.xml"]
cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", main_configs=CONFIG_FILES, with_mysql=True, dictionaries=DICTS
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
        instance.query("CREATE DATABASE IF NOT EXISTS test")

        # Create database in ClickChouse using MySQL protocol (will be used for data insertion)
        instance.query(
            "CREATE DATABASE clickhouse_mysql ENGINE = MySQL('mysql57:3306', 'test', 'root', 'clickhouse')"
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
        """
    CREATE DICTIONARY test_dictionary_custom_query
    (
        id UInt64,
        value_1 String,
        value_2 String
    )
    PRIMARY KEY id
    LAYOUT(FLAT())
    SOURCE(MYSQL(
        HOST 'mysql57'
        PORT 3306
        USER 'root'
        PASSWORD 'clickhouse'
        QUERY $doc$SELECT id, value_1, value_2 FROM test.test_table_1 INNER JOIN test.test_table_2 USING (id);$doc$))
    LIFETIME(0)
    """
    )

    result = query("SELECT id, value_1, value_2 FROM test_dictionary_custom_query")

    assert result == "1\tValue_1\tValue_2\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

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
        """
    CREATE DICTIONARY test_dictionary_custom_query
    (
        id UInt64,
        value_1 String,
        value_2 String
    )
    PRIMARY KEY id
    LAYOUT(DIRECT())
    SOURCE(MYSQL(
        HOST 'mysql57'
        PORT 3306
        USER 'root'
        PASSWORD 'clickhouse'
        QUERY $doc$SELECT id, value_1, value_2 FROM test.test_table_1 INNER JOIN test.test_table_2 USING (id) WHERE {condition};$doc$))
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
        """
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
        HOST 'mysql57'
        PORT 3306
        USER 'root'
        PASSWORD 'clickhouse'
        QUERY $doc$SELECT id, id_key, value_1, value_2 FROM test.test_table_1 INNER JOIN test.test_table_2 USING (id, id_key) WHERE {condition};$doc$))
    """
    )

    result = query(
        "SELECT dictGet('test_dictionary_custom_query', ('value_1', 'value_2'), (toUInt64(1), 'Key'))"
    )

    assert result == "('Value_1','Value_2')\n"

    query("DROP DICTIONARY test_dictionary_custom_query;")

    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_1;")
    execute_mysql_query(mysql_connection, "DROP TABLE test.test_table_2;")


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
                    password="clickhouse",
                    host=started_cluster.mysql_ip,
                    port=started_cluster.mysql_port,
                )
            else:
                conn.ping(reconnect=True)
            logging.debug(
                f"MySQL Connection establised: {started_cluster.mysql_ip}:{started_cluster.mysql_port}"
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

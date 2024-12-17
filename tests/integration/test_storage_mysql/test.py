import threading
import time
from contextlib import contextmanager

## sudo -H pip install PyMySQL
import pymysql.cursors
import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    with_mysql8=True,
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_mysql_cluster=True
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    with_mysql8=True,
)

create_table_sql_template = """
    CREATE TABLE `clickhouse`.`{}` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NOT NULL,
    `age` int  NOT NULL default 0,
    `money` int NOT NULL default 0,
    `source` enum('IP','URL') DEFAULT 'IP',
    PRIMARY KEY (`id`)) ENGINE=InnoDB;
    """

drop_table_sql_template = """
    DROP TABLE IF EXISTS `clickhouse`.`{}`;
    """


def get_mysql_conn(started_cluster, host):
    conn = pymysql.connect(
        user="root", password="clickhouse", host=host, port=started_cluster.mysql8_port
    )
    return conn


def create_mysql_table(conn, tableName):
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql_template.format(tableName))


def drop_mysql_table(conn, tableName):
    with conn.cursor() as cursor:
        cursor.execute(drop_table_sql_template.format(tableName))


def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute("DROP DATABASE IF EXISTS {}".format(name))
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        conn = get_mysql_conn(cluster, cluster.mysql8_ip)
        create_mysql_db(conn, "clickhouse")

        ## create mysql db and table
        conn1 = get_mysql_conn(cluster, cluster.mysql2_ip)
        create_mysql_db(conn1, "clickhouse")
        yield cluster

    finally:
        cluster.shutdown()


def test_many_connections(started_cluster):
    table_name = "test_many_connections"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse');
""".format(
            table_name, table_name
        )
    )

    node1.query(
        "INSERT INTO {} (id, name) SELECT number, concat('name_', toString(number)) from numbers(10) ".format(
            table_name
        )
    )

    query = "SELECT count() FROM ("
    for i in range(24):
        query += "SELECT id FROM {t} UNION ALL "
    query += "SELECT id FROM {t})"

    assert node1.query(query.format(t=table_name)) == "250\n"
    drop_mysql_table(conn, table_name)
    conn.close()


def test_insert_select(started_cluster):
    table_name = "test_insert_select"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse');
""".format(
            table_name, table_name
        )
    )
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name
        )
    )
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == "10000"
    assert (
        node1.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == "30000"
    )
    conn.close()


def test_replace_select(started_cluster):
    table_name = "test_replace_select"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse', 1);
""".format(
            table_name, table_name
        )
    )
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name
        )
    )
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name
        )
    )
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == "10000"
    assert (
        node1.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == "30000"
    )
    conn.close()


def test_insert_on_duplicate_select(started_cluster):
    table_name = "test_insert_on_duplicate_select"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse', 0, 'update money = money + values(money)');
""".format(
            table_name, table_name
        )
    )
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name
        )
    )
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name
        )
    )
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == "10000"
    assert (
        node1.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == "60000"
    )
    conn.close()


def test_where(started_cluster):
    table_name = "test_where"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)
    node1.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse');
""".format(
            table_name, table_name
        )
    )
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name
        )
    )
    assert (
        node1.query(
            "SELECT count() FROM {} WHERE name LIKE '%name_%'".format(table_name)
        ).rstrip()
        == "10000"
    )
    assert (
        node1.query(
            "SELECT count() FROM {} WHERE name NOT LIKE '%tmp_%'".format(table_name)
        ).rstrip()
        == "10000"
    )
    assert (
        node1.query(
            "SELECT count() FROM {} WHERE money IN (1, 2, 3)".format(table_name)
        ).rstrip()
        == "10000"
    )
    assert (
        node1.query(
            "SELECT count() FROM {} WHERE money IN (1, 2, 4, 5, 6)".format(table_name)
        ).rstrip()
        == "0"
    )
    assert (
        node1.query(
            "SELECT count() FROM {} WHERE money NOT IN (1, 2, 4, 5, 6)".format(
                table_name
            )
        ).rstrip()
        == "10000"
    )
    assert (
        node1.query(
            "SELECT count() FROM {} WHERE name LIKE concat('name_', toString(1))".format(
                table_name
            )
        ).rstrip()
        == "1"
    )
    conn.close()


def test_table_function(started_cluster):
    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, "table_function")
    create_mysql_table(conn, "table_function")
    table_function = (
        "mysql('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse')".format(
            "table_function"
        )
    )
    assert node1.query("SELECT count() FROM {}".format(table_function)).rstrip() == "0"
    node1.query(
        "INSERT INTO {} (id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000)".format(
            "TABLE FUNCTION " + table_function
        )
    )
    assert (
        node1.query("SELECT count() FROM {}".format(table_function)).rstrip() == "10000"
    )
    assert (
        node1.query(
            "SELECT sum(c) FROM ("
            "SELECT count() as c FROM {} WHERE id % 3 == 0"
            " UNION ALL SELECT count() as c FROM {} WHERE id % 3 == 1"
            " UNION ALL SELECT count() as c FROM {} WHERE id % 3 == 2)".format(
                table_function, table_function, table_function
            )
        ).rstrip()
        == "10000"
    )
    assert (
        node1.query("SELECT sum(`money`) FROM {}".format(table_function)).rstrip()
        == "30000"
    )
    node1.query(
        "INSERT INTO {} (id, name, age, money) SELECT id + 100000, name, age, money FROM {}".format(
            "TABLE FUNCTION " + table_function, table_function
        )
    )
    assert (
        node1.query("SELECT sum(`money`) FROM {}".format(table_function)).rstrip()
        == "60000"
    )
    conn.close()


def test_schema_inference(started_cluster):
    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, "inference_table")

    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE clickhouse.inference_table (id INT PRIMARY KEY, data BINARY(16) NOT NULL)"
        )

    parameters = "'mysql80:3306', 'clickhouse', 'inference_table', 'root', 'clickhouse'"

    node1.query(
        f"CREATE TABLE mysql_schema_inference_engine ENGINE=MySQL({parameters})"
    )
    node1.query(f"CREATE TABLE mysql_schema_inference_function AS mysql({parameters})")

    expected = "id\tInt32\t\t\t\t\t\ndata\tFixedString(16)\t\t\t\t\t\n"
    assert node1.query("DESCRIBE TABLE mysql_schema_inference_engine") == expected
    assert node1.query("DESCRIBE TABLE mysql_schema_inference_function") == expected

    node1.query("DROP TABLE mysql_schema_inference_engine")
    node1.query("DROP TABLE mysql_schema_inference_function")

    drop_mysql_table(conn, "inference_table")


def test_binary_type(started_cluster):
    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, "binary_type")

    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE clickhouse.binary_type (id INT PRIMARY KEY, data BINARY(16) NOT NULL)"
        )
    table_function = (
        "mysql('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse')".format(
            "binary_type"
        )
    )
    node1.query(
        "INSERT INTO {} VALUES (42, 'clickhouse')".format(
            "TABLE FUNCTION " + table_function
        )
    )
    assert (
        node1.query("SELECT * FROM {}".format(table_function))
        == "42\tclickhouse\\0\\0\\0\\0\\0\\0\n"
    )
    drop_mysql_table(conn, "binary_type")


def test_enum_type(started_cluster):
    table_name = "test_enum_type"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)
    node1.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32, source Enum8('IP' = 1, 'URL' = 2)) ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse', 1);
""".format(
            table_name, table_name
        )
    )
    node1.query(
        "INSERT INTO {} (id, name, age, money, source) VALUES (1, 'name', 0, 0, 'URL')".format(
            table_name
        )
    )
    assert (
        node1.query("SELECT source FROM {} LIMIT 1".format(table_name)).rstrip()
        == "URL"
    )
    conn.close()


def test_mysql_distributed(started_cluster):
    table_name = "test_replicas"

    conn1 = get_mysql_conn(started_cluster, started_cluster.mysql8_ip)
    conn2 = get_mysql_conn(started_cluster, started_cluster.mysql2_ip)
    conn3 = get_mysql_conn(started_cluster, started_cluster.mysql3_ip)
    conn4 = get_mysql_conn(started_cluster, started_cluster.mysql4_ip)

    create_mysql_db(conn1, "clickhouse")
    create_mysql_db(conn2, "clickhouse")
    create_mysql_db(conn3, "clickhouse")
    create_mysql_db(conn4, "clickhouse")

    create_mysql_table(conn1, table_name)
    create_mysql_table(conn2, table_name)
    create_mysql_table(conn3, table_name)
    create_mysql_table(conn4, table_name)

    node2.query("DROP TABLE IF EXISTS test_replicas")

    # Storage with with 3 replicas
    node2.query(
        """
        CREATE TABLE test_replicas
        (id UInt32, name String, age UInt32, money UInt32)
        ENGINE = MySQL('mysql{2|3|4}:3306', 'clickhouse', 'test_replicas', 'root', 'clickhouse'); """
    )

    # Fill remote tables with different data to be able to check
    nodes = [node1, node2, node2, node2]
    for i in range(1, 5):
        nodes[i - 1].query("DROP TABLE IF EXISTS test_replica{}".format(i))
        nodes[i - 1].query(
            """
            CREATE TABLE test_replica{}
            (id UInt32, name String, age UInt32, money UInt32)
            ENGINE = MySQL('mysql{}:3306', 'clickhouse', 'test_replicas', 'root', 'clickhouse');""".format(
                i, 80 if i == 1 else i
            )
        )
        nodes[i - 1].query(
            "INSERT INTO test_replica{} (id, name) SELECT number, 'host{}' from numbers(10) ".format(
                i, i
            )
        )

    # test multiple ports parsing
    result = node2.query(
        """SELECT DISTINCT(name) FROM mysql('mysql{80|2|3}:3306', 'clickhouse', 'test_replicas', 'root', 'clickhouse'); """
    )
    assert result == "host1\n" or result == "host2\n" or result == "host3\n"
    result = node2.query(
        """SELECT DISTINCT(name) FROM mysql('mysql80:3306|mysql2:3306|mysql3:3306', 'clickhouse', 'test_replicas', 'root', 'clickhouse'); """
    )
    assert result == "host1\n" or result == "host2\n" or result == "host3\n"

    # check all replicas are traversed
    query = "SELECT * FROM ("
    for i in range(3):
        query += "SELECT name FROM test_replicas UNION DISTINCT "
    query += "SELECT name FROM test_replicas) ORDER BY name"

    result = node2.query(query)
    assert result == "host2\nhost3\nhost4\n"

    # Storage with with two shards, each has 2 replicas
    node2.query("DROP TABLE IF EXISTS test_shards")

    node2.query(
        """
        CREATE TABLE test_shards
        (id UInt32, name String, age UInt32, money UInt32)
        ENGINE = ExternalDistributed('MySQL', 'mysql{80|2}:3306,mysql{3|4}:3306', 'clickhouse', 'test_replicas', 'root', 'clickhouse'); """
    )

    # Check only one replica in each shard is used
    result = node2.query("SELECT DISTINCT(name) FROM test_shards ORDER BY name")
    assert result == "host1\nhost3\n"

    # check all replicas are traversed
    query = "SELECT name FROM ("
    for i in range(3):
        query += "SELECT name FROM test_shards UNION DISTINCT "
    query += "SELECT name FROM test_shards) ORDER BY name"
    result = node2.query(query)
    assert result == "host1\nhost2\nhost3\nhost4\n"

    # disconnect mysql
    started_cluster.pause_container("mysql80")
    result = node2.query("SELECT DISTINCT(name) FROM test_shards ORDER BY name")
    started_cluster.unpause_container("mysql80")
    assert result == "host2\nhost4\n" or result == "host3\nhost4\n"


def test_external_settings(started_cluster):
    table_name = "test_external_settings"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    conn = get_mysql_conn(started_cluster, started_cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node3.query(f"DROP TABLE IF EXISTS {table_name}")
    node3.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse');
""".format(
            table_name, table_name
        )
    )
    node3.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(100) ".format(
            table_name
        )
    )
    assert node3.query("SELECT count() FROM {}".format(table_name)).rstrip() == "100"
    assert node3.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == "300"
    node3.query(
        "select value from system.settings where name = 'max_block_size' FORMAT TSV"
    ) == "2\n"
    node3.query(
        "select value from system.settings where name = 'external_storage_max_read_rows' FORMAT TSV"
    ) == "0\n"
    assert (
        node3.query(
            "SELECT COUNT(DISTINCT blockNumber()) FROM {} FORMAT TSV".format(table_name)
        )
        == "50\n"
    )
    conn.close()


def test_settings_connection_wait_timeout(started_cluster):
    table_name = "test_settings_connection_wait_timeout"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    wait_timeout = 2

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        """
        CREATE TABLE {}
        (
            id UInt32,
            name String,
            age UInt32,
            money UInt32
        )
        ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse')
        SETTINGS connection_wait_timeout={}, connection_pool_size=1
        """.format(
            table_name, table_name, wait_timeout
        )
    )

    node1.query(
        "INSERT INTO {} (id, name) SELECT number, concat('name_', toString(number)) from numbers(10) ".format(
            table_name
        )
    )

    worker_started_event = threading.Event()

    def worker():
        worker_started_event.set()
        node1.query(
            "SELECT 1, sleepEachRow(1) FROM {} SETTINGS max_threads=1".format(
                table_name
            )
        )

    worker_thread = threading.Thread(target=worker)
    worker_thread.start()

    # ensure that first query started in worker_thread
    assert worker_started_event.wait(10)
    time.sleep(1)

    started = time.time()
    with pytest.raises(
        QueryRuntimeException,
        match=r"Exception: mysqlxx::Pool is full \(connection_wait_timeout is exceeded\)",
    ):
        node1.query(
            "SELECT 2, sleepEachRow(1) FROM {} SETTINGS max_threads=1".format(
                table_name
            )
        )
    ended = time.time()
    assert (ended - started) >= wait_timeout

    worker_thread.join()

    drop_mysql_table(conn, table_name)
    conn.close()


def test_predefined_connection_configuration(started_cluster):
    conn = get_mysql_conn(started_cluster, started_cluster.mysql8_ip)
    table_name = "test_table"
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        """
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id UInt32, name String, age UInt32, money UInt32)
        ENGINE MySQL(mysql1);
    """
    )
    node1.query(
        "INSERT INTO test_table (id, name, money) select number, toString(number), number from numbers(100)"
    )
    assert node1.query(f"SELECT count() FROM test_table").rstrip() == "100"

    node1.query(
        """
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id UInt32, name String, age UInt32, money UInt32)
        ENGINE MySQL(mysql1, replace_query=1);
    """
    )
    node1.query(
        "INSERT INTO test_table (id, name, money) select number, toString(number), number from numbers(100)"
    )
    node1.query(
        "INSERT INTO test_table (id, name, money) select number, toString(number), number from numbers(100)"
    )
    assert node1.query(f"SELECT count() FROM test_table").rstrip() == "100"

    node1.query_and_get_error(
        """
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id UInt32, name String, age UInt32, money UInt32)
        ENGINE MySQL(mysql1, query=1);
    """
    )
    node1.query_and_get_error(
        """
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id UInt32, name String, age UInt32, money UInt32)
        ENGINE MySQL(mysql1, replace_query=1, on_duplicate_clause='kek');
    """
    )
    node1.query_and_get_error(
        """
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id UInt32, name String, age UInt32, money UInt32)
        ENGINE MySQL(fff);
    """
    )
    node1.query_and_get_error(
        """
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id UInt32, name String, age UInt32, money UInt32)
        ENGINE MySQL(mysql2);
    """
    )

    node1.query(
        """
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id UInt32, name String, age UInt32, money UInt32)
        ENGINE MySQL(mysql3, port=3306);
    """
    )
    assert node1.query(f"SELECT count() FROM test_table").rstrip() == "100"

    assert "Connection pool cannot have zero size" in node1.query_and_get_error(
        "SELECT count() FROM mysql(mysql1, `table`='test_table', connection_pool_size=0)"
    )
    assert "Connection pool cannot have zero size" in node1.query_and_get_error(
        "SELECT count() FROM mysql(mysql4)"
    )
    assert (
        int(node1.query("SELECT count() FROM mysql(mysql4, connection_pool_size=1)"))
        == 100
    )


# Regression for (k, v) IN ((k, v))
def test_mysql_in(started_cluster):
    table_name = "test_mysql_in"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        """
        CREATE TABLE {}
        (
            id UInt32,
            name String,
            age UInt32,
            money UInt32
        )
        ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse')
        """.format(
            table_name, table_name
        )
    )

    node1.query(
        "INSERT INTO {} (id, name) SELECT number, concat('name_', toString(number)) from numbers(10) ".format(
            table_name
        )
    )
    node1.query("SELECT * FROM {} WHERE (id) IN (1)".format(table_name))
    node1.query("SELECT * FROM {} WHERE (id) IN (1, 2)".format(table_name))
    node1.query(
        "SELECT * FROM {} WHERE (id, name) IN ((1, 'name_1'))".format(table_name)
    )
    node1.query(
        "SELECT * FROM {} WHERE (id, name) IN ((1, 'name_1'),(1, 'name_1'))".format(
            table_name
        )
    )

    drop_mysql_table(conn, table_name)
    conn.close()


def test_mysql_null(started_cluster):
    table_name = "test_mysql_in"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE `clickhouse`.`{}` (
            `id` int(11) NOT NULL,
            `money` int NULL default NULL,
            PRIMARY KEY (`id`)) ENGINE=InnoDB;
        """.format(
                table_name
            )
        )

    node1.query(
        """
        CREATE TABLE {}
        (
            id UInt32,
            money Nullable(UInt32)
        )
        ENGINE = MySQL('mysql80:3306', 'clickhouse', '{}', 'root', 'clickhouse')
        """.format(
            table_name, table_name
        )
    )

    node1.query(
        "INSERT INTO {} (id, money) SELECT number, if(number%2, NULL, 1) from numbers(10) ".format(
            table_name
        )
    )

    assert (
        int(
            node1.query(
                "SELECT count() FROM {} WHERE money IS NULL SETTINGS external_table_strict_query=1".format(
                    table_name
                )
            )
        )
        == 5
    )
    assert (
        int(
            node1.query(
                "SELECT count() FROM {} WHERE money IS NOT NULL SETTINGS external_table_strict_query=1".format(
                    table_name
                )
            )
        )
        == 5
    )

    drop_mysql_table(conn, table_name)
    conn.close()


def test_settings(started_cluster):
    table_name = "test_settings"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")
    wait_timeout = 123
    rw_timeout = 10123001
    connect_timeout = 10123002
    connection_pool_size = 1

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    create_mysql_table(conn, table_name)

    node1.query(
        f"""
        CREATE TABLE {table_name}
        (
            id UInt32,
            name String,
            age UInt32,
            money UInt32
        )
        ENGINE = MySQL('mysql80:3306', 'clickhouse', '{table_name}', 'root', 'clickhouse')
        SETTINGS connection_wait_timeout={wait_timeout}, connect_timeout={connect_timeout}, read_write_timeout={rw_timeout}, connection_pool_size={connection_pool_size}
        """
    )

    node1.query(f"SELECT * FROM {table_name}")
    assert node1.contains_in_log(
        f"with settings: connect_timeout={connect_timeout}, read_write_timeout={rw_timeout}"
    )

    rw_timeout = 20123001
    connect_timeout = 20123002
    node1.query(f"SELECT * FROM mysql(mysql_with_settings, table='test_settings')")
    assert node1.contains_in_log(
        f"with settings: connect_timeout={connect_timeout}, read_write_timeout={rw_timeout}"
    )

    rw_timeout = 30123001
    connect_timeout = 30123002
    node1.query(
        f"""
        SELECT *
            FROM mysql('mysql80:3306', 'clickhouse', '{table_name}', 'root', 'clickhouse',
                       SETTINGS
                           connection_wait_timeout={wait_timeout},
                           connect_timeout={connect_timeout},
                           read_write_timeout={rw_timeout},
                           connection_pool_size={connection_pool_size})
    """
    )
    assert node1.contains_in_log(
        f"with settings: connect_timeout={connect_timeout}, read_write_timeout={rw_timeout}"
    )

    node1.query("DROP DATABASE IF EXISTS m")
    node1.query("DROP DATABASE IF EXISTS mm")

    rw_timeout = 40123001
    connect_timeout = 40123002
    node1.query(
        f"""
        CREATE DATABASE m
        ENGINE = MySQL(mysql_with_settings, connection_wait_timeout={wait_timeout}, connect_timeout={connect_timeout}, read_write_timeout={rw_timeout}, connection_pool_size={connection_pool_size})
    """
    )
    assert node1.contains_in_log(
        f"with settings: connect_timeout={connect_timeout}, read_write_timeout={rw_timeout}"
    )

    rw_timeout = 50123001
    connect_timeout = 50123002
    node1.query(
        f"""
        CREATE DATABASE mm ENGINE = MySQL('mysql80:3306', 'clickhouse', 'root', 'clickhouse')
            SETTINGS
                connection_wait_timeout={wait_timeout},
                connect_timeout={connect_timeout},
                read_write_timeout={rw_timeout},
                connection_pool_size={connection_pool_size}
    """
    )
    assert node1.contains_in_log(
        f"with settings: connect_timeout={connect_timeout}, read_write_timeout={rw_timeout}"
    )

    node1.query("DROP DATABASE m")
    node1.query("DROP DATABASE mm")

    drop_mysql_table(conn, table_name)
    conn.close()


def test_mysql_point(started_cluster):
    table_name = "test_mysql_point"
    node1.query(f"DROP TABLE IF EXISTS {table_name}")

    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE `clickhouse`.`{table_name}` (
            `id` int NOT NULL,
            `point` Point NOT NULL,
            PRIMARY KEY (`id`)) ENGINE=InnoDB;
        """
        )
        cursor.execute(
            f"INSERT INTO `clickhouse`.`{table_name}` SELECT 1, Point(15, 20)"
        )
        assert 1 == cursor.execute(f"SELECT count(*) FROM `clickhouse`.`{table_name}`")

    conn.commit()

    result = node1.query(
        f"DESCRIBE mysql('mysql80:3306', 'clickhouse', '{table_name}', 'root', 'clickhouse')"
    )
    assert result.strip() == "id\tInt32\t\t\t\t\t\npoint\tPoint"

    assert 1 == int(
        node1.query(
            f"SELECT count() FROM mysql('mysql80:3306', 'clickhouse', '{table_name}', 'root', 'clickhouse')"
        )
    )
    assert (
        "(15,20)"
        == node1.query(
            f"SELECT point FROM mysql('mysql80:3306', 'clickhouse', '{table_name}', 'root', 'clickhouse')"
        ).strip()
    )

    node1.query("DROP TABLE IF EXISTS test")
    node1.query(
        f"CREATE TABLE test (id Int32, point Point) Engine=MySQL('mysql80:3306', 'clickhouse', '{table_name}', 'root', 'clickhouse')"
    )
    assert "(15,20)" == node1.query(f"SELECT point FROM test").strip()

    drop_mysql_table(conn, table_name)
    conn.close()


def test_joins(started_cluster):
    conn = get_mysql_conn(started_cluster, cluster.mysql8_ip)
    drop_mysql_table(conn, "test_joins_mysql_users")
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE clickhouse.test_joins_mysql_users (id INT NOT NULL, name varchar(50) NOT NULL, created TIMESTAMP, PRIMARY KEY (`id`)) ENGINE=InnoDB;"
        )
        cursor.execute(
            f"INSERT INTO clickhouse.test_joins_mysql_users VALUES (469722, 'user@example.com', '2019-08-30 07:55:01')"
        )

    drop_mysql_table(conn, "test_joins_mysql_tickets")
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE clickhouse.test_joins_mysql_tickets (id INT NOT NULL, subject varchar(50), created TIMESTAMP, creator INT NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB;"
        )
        cursor.execute(
            f"INSERT INTO clickhouse.test_joins_mysql_tickets VALUES (281607, 'Feedback', '2024-06-25 12:09:41', 469722)"
        )

    conn.commit()

    node1.query("DROP TABLE IF EXISTS test_joins_table_users")
    node1.query("DROP TABLE IF EXISTS test_joins_table_tickets")

    node1.query(
        """
        CREATE TABLE test_joins_table_users
        (
            `id` Int32,
            `Name` String,
            `Created` Nullable(DateTime)
        )
        ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_joins_mysql_users', 'root', 'clickhouse');
        """
    )

    node1.query(
        """
        CREATE TABLE test_joins_table_tickets
        (
            `id` Int32,
            `Subject` Nullable(String),
            `Created` Nullable(DateTime),
            `Creator` Int32
        )
        ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_joins_mysql_tickets', 'root', 'clickhouse');
        """
    )

    node1.query(
        """
        SELECT test_joins_table_tickets.id, Subject, test_joins_table_tickets.Created, Name
        FROM test_joins_table_tickets
        LEFT JOIN test_joins_table_users ON test_joins_table_tickets.Creator = test_joins_table_users.id
        WHERE test_joins_table_tickets.Created = '2024-06-25 12:09:41'
        """
    ) == "281607\tFeedback\t2024-06-25 12:09:41\tuser@example.com\n"

    node1.query("DROP TABLE test_joins_table_users")
    node1.query("DROP TABLE test_joins_table_tickets")


if __name__ == "__main__":
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")

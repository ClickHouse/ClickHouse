import pymysql.cursors
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
configs = ["configs/remote_servers.xml", "configs/backups_disk.xml"]

node1 = cluster.add_instance(
    "replica1",
    with_zookeeper=True,
    with_mysql8=True,
    main_configs=configs,
    external_dirs=["/backups/"],
)
node2 = cluster.add_instance(
    "replica2",
    with_zookeeper=True,
    with_mysql8=True,
    main_configs=configs,
    external_dirs=["/backups/"],
)
node3 = cluster.add_instance(
    "replica3",
    with_zookeeper=True,
    with_mysql8=True,
    main_configs=configs,
    external_dirs=["/backups/"],
)
nodes = [node1, node2, node3]

backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


def cleanup_nodes(nodes, dbname):
    for node in nodes:
        node.query(f"DROP DATABASE IF EXISTS {dbname} SYNC")


def fill_nodes(nodes, dbname):
    cleanup_nodes(nodes, dbname)
    for node in nodes:
        node.query(
            f"CREATE DATABASE {dbname} ENGINE = Replicated('/clickhouse/databases/{dbname}', 'default', '{node.name}')"
        )


def drop_mysql_table(conn, tableName):
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `clickhouse`.`{tableName}`")


def get_mysql_conn(cluster):
    conn = pymysql.connect(
        user="root",
        password="clickhouse",
        host=cluster.mysql8_ip,
        port=cluster.mysql8_port,
    )
    return conn


def fill_tables(cluster, dbname):
    fill_nodes(nodes, dbname)

    node1.query(
        f"""CREATE TABLE {dbname}.example_s3_engine_table (name String, value UInt32)
ENGINE = S3('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'gzip')
SETTINGS input_format_with_names_use_header = 0"""
    )

    conn = get_mysql_conn(cluster)

    with conn.cursor() as cursor:
        cursor.execute("DROP DATABASE IF EXISTS clickhouse")
        cursor.execute("CREATE DATABASE clickhouse")
        cursor.execute("DROP TABLE IF EXISTS clickhouse.inference_table")
        cursor.execute(
            "CREATE TABLE clickhouse.inference_table (id INT PRIMARY KEY, data BINARY(16) NOT NULL)"
        )
        cursor.execute(
            "INSERT INTO clickhouse.inference_table VALUES (100, X'9fad5e9eefdfb449')"
        )
        conn.commit()

    parameters = "'mysql80:3306', 'clickhouse', 'inference_table', 'root', 'clickhouse'"

    node1.query(
        f"CREATE TABLE {dbname}.mysql_schema_inference_engine ENGINE=MySQL({parameters})"
    )
    node1.query(
        f"CREATE TABLE {dbname}.mysql_schema_inference_function AS mysql({parameters})"
    )

    node1.query(f"CREATE TABLE {dbname}.merge_tree (id UInt64, b String) ORDER BY id")
    node1.query(f"INSERT INTO {dbname}.merge_tree VALUES (100, 'abc')")

    expected = "id\tInt32\t\t\t\t\t\ndata\tFixedString(16)\t\t\t\t\t\n"
    assert (
        node1.query(f"DESCRIBE TABLE {dbname}.mysql_schema_inference_engine")
        == expected
    )
    assert (
        node1.query(f"DESCRIBE TABLE {dbname}.mysql_schema_inference_function")
        == expected
    )
    assert node1.query(f"SELECT id FROM mysql({parameters})") == "100\n"
    assert (
        node1.query(f"SELECT id FROM {dbname}.mysql_schema_inference_engine") == "100\n"
    )
    assert (
        node1.query(f"SELECT id FROM {dbname}.mysql_schema_inference_function")
        == "100\n"
    )
    assert node1.query(f"SELECT id FROM {dbname}.merge_tree") == "100\n"

    node1.query(
        f"CREATE DICTIONARY {dbname}.dict1 (id INT, data String) PRIMARY KEY id "
        f"SOURCE(MYSQL(HOST 'mysql80' PORT 3306 USER 'root' PASSWORD 'clickhouse' DB 'clickhouse' TABLE 'inference_table'))"
        f"LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 10)"
    )

    node1.query(
        f"CREATE DICTIONARY {dbname}.dict2 (name String, value UInt32) PRIMARY KEY value "
        f"SOURCE(CLICKHOUSE(HOST '127.0.0.2' PORT 9000 USER 'default' PASSWORD '' DB '{dbname}' TABLE 'example_s3_engine_table'))"
        f"LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 10)"
    )

    node1.query(
        f"CREATE DICTIONARY {dbname}.dict3 (name String, value UInt32) PRIMARY KEY value "
        f"SOURCE(CLICKHOUSE(USER 'default' PASSWORD '' DB '{dbname}' TABLE 'example_s3_engine_table'))"
        f"LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 10)"
    )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_restore_table(start_cluster):
    fill_tables(cluster, "replicated")
    backup_name = new_backup_name()
    node2.query(f"SYSTEM SYNC DATABASE REPLICA replicated")

    node2.query(f"BACKUP DATABASE replicated TO {backup_name}")

    node2.query("DROP DICTIONARY IF EXISTS replicated.dict3 SYNC")
    node2.query("DROP DICTIONARY IF EXISTS replicated.dict2 SYNC")
    node2.query("DROP DICTIONARY IF EXISTS replicated.dict1 SYNC")
    node2.query("DROP TABLE replicated.example_s3_engine_table")
    node2.query("DROP TABLE replicated.mysql_schema_inference_engine")
    node2.query("DROP TABLE replicated.mysql_schema_inference_function")

    node3.query(f"SYSTEM SYNC DATABASE REPLICA replicated")

    assert node3.query("EXISTS replicated.mysql_schema_inference_engine") == "0\n"
    assert node3.query("EXISTS replicated.mysql_schema_inference_function") == "0\n"

    node3.query(
        f"RESTORE DATABASE replicated FROM {backup_name} SETTINGS allow_different_database_def=true"
    )
    node1.query(f"SYSTEM SYNC DATABASE REPLICA replicated")

    assert (
        node1.query(
            "SELECT engine FROM system.tables where database = 'replicated' and name = 'example_s3_engine_table'"
        )
        == "S3\n"
    )

    assert (
        node1.query(
            "SELECT count(), sum(id) FROM replicated.mysql_schema_inference_engine"
        )
        == "1\t100\n"
    )
    assert (
        node1.query(
            "SELECT count(), sum(id) FROM replicated.mysql_schema_inference_function"
        )
        == "1\t100\n"
    )
    assert (
        node1.query("SELECT count(), sum(id) FROM replicated.merge_tree") == "1\t100\n"
    )
    cleanup_nodes(nodes, "replicated")


def test_restore_table_null(start_cluster):
    fill_tables(cluster, "replicated2")

    backup_name = new_backup_name()
    node2.query(f"SYSTEM SYNC DATABASE REPLICA replicated2")

    node2.query(f"BACKUP DATABASE replicated2 TO {backup_name}")

    node2.query("DROP DICTIONARY IF EXISTS replicated2.dict3 SYNC")
    node2.query("DROP DICTIONARY IF EXISTS replicated2.dict2 SYNC")
    node2.query("DROP DICTIONARY IF EXISTS replicated2.dict1 SYNC")
    node2.query("DROP TABLE replicated2.example_s3_engine_table")
    node2.query("DROP TABLE replicated2.mysql_schema_inference_engine")
    node2.query("DROP TABLE replicated2.mysql_schema_inference_function")

    node3.query(f"SYSTEM SYNC DATABASE REPLICA replicated2")

    assert node3.query("EXISTS replicated2.mysql_schema_inference_engine") == "0\n"
    assert node3.query("EXISTS replicated2.mysql_schema_inference_function") == "0\n"

    node3.query(
        f"RESTORE DATABASE replicated2 FROM {backup_name} SETTINGS allow_different_database_def=1, allow_different_table_def=1 "
        f"SETTINGS restore_replace_external_engines_to_null=1, restore_replace_external_table_functions_to_null=1, restore_replace_external_dictionary_source_to_null=1"
    )
    node1.query(f"SYSTEM SYNC DATABASE REPLICA replicated2")

    assert (
        node1.query(
            "SELECT engine FROM system.tables where database = 'replicated2' and name = 'example_s3_engine_table'"
        )
        == "Null\n"
    )

    assert (
        node1.query(
            "SELECT count(), sum(id) FROM replicated2.mysql_schema_inference_engine"
        )
        == "0\t0\n"
    )
    assert (
        node1.query(
            "SELECT count(), sum(id) FROM replicated2.mysql_schema_inference_function"
        )
        == "0\t0\n"
    )
    assert (
        node1.query("SELECT count(), sum(id) FROM replicated2.merge_tree") == "1\t100\n"
    )
    assert (
        node1.query(
            "SELECT engine FROM system.tables where database = 'replicated2' and name like '%mysql%'"
        )
        == "Null\nNull\n"
    )
    assert (
        node1.query(
            "SELECT engine FROM system.tables where database = 'replicated2' and name like '%merge_tree%'"
        )
        == "MergeTree\n"
    )
    assert "SOURCE(NULL())" in node1.query("SHOW CREATE replicated2.dict1")
    assert "SOURCE(NULL())" in node1.query("SHOW CREATE replicated2.dict1")
    assert "SOURCE(CLICKHOUSE(" in node1.query("SHOW CREATE replicated2.dict3")
    cleanup_nodes(nodes, "replicated2")

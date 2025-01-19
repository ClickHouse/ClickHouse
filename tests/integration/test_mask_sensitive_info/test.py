import random
import re
import string

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/overrides.xml",
    ],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    with_azurite=True,
)
base_search_query = "SELECT COUNT() FROM system.query_log WHERE query LIKE "


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def check_logs(must_contain=[], must_not_contain=[]):
    node.query("SYSTEM FLUSH LOGS")

    for str in must_contain:
        escaped_str = (
            str.replace("`", "\\`")
            .replace("[", "\\[")
            .replace("]", "\\]")
            .replace("*", "\\*")
        )
        assert node.contains_in_log(escaped_str, exclusion_substring=base_search_query)

    for str in must_not_contain:
        escaped_str = (
            str.replace("`", "\\`")
            .replace("[", "\\[")
            .replace("]", "\\]")
            .replace("*", "\\*")
        )
        assert not node.contains_in_log(
            escaped_str, exclusion_substring=base_search_query
        )

    for str in must_contain:
        escaped_str = str.replace("'", "\\'")
        assert system_query_log_contains_search_pattern(escaped_str)

    for str in must_not_contain:
        escaped_str = str.replace("'", "\\'")
        assert not system_query_log_contains_search_pattern(escaped_str)


# Returns true if "system.query_log" has a query matching a specified pattern.
def system_query_log_contains_search_pattern(search_pattern):
    return (
        int(
            node.query(
                f"{base_search_query}'%{search_pattern}%' AND query NOT LIKE '{base_search_query}%'"
            ).strip()
        )
        >= 1
    )


def new_password(len=16):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(len)
    )


show_secrets = "SETTINGS format_display_secrets_in_show_and_select"


def test_create_alter_user():
    password = new_password()

    node.query(f"CREATE USER u1 IDENTIFIED BY '{password}' SETTINGS custom_a = 'a'")
    node.query(
        f"ALTER USER u1 IDENTIFIED BY '{password}{password}' SETTINGS custom_b = 'b'"
    )
    node.query(
        f"CREATE USER u2 IDENTIFIED WITH plaintext_password BY '{password}' SETTINGS custom_c = 'c'"
    )

    assert (
        node.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH sha256_password SETTINGS custom_b = \\'b\\'\n"
    )
    assert (
        node.query("SHOW CREATE USER u2")
        == "CREATE USER u2 IDENTIFIED WITH plaintext_password SETTINGS custom_c = \\'c\\'\n"
    )

    check_logs(
        must_contain=[
            "CREATE USER u1 IDENTIFIED",
            "ALTER USER u1 IDENTIFIED",
            "CREATE USER u2 IDENTIFIED WITH plaintext_password",
        ],
        must_not_contain=[
            password,
            "IDENTIFIED BY",
            "IDENTIFIED WITH plaintext_password BY",
        ],
    )

    assert "BY" in node.query(f"SHOW CREATE USER u1 {show_secrets}=1")
    assert "BY" in node.query(f"SHOW CREATE USER u2 {show_secrets}=1")

    node.query("DROP USER u1, u2")


def check_secrets_for_tables(test_cases, password):
    for table_name, query, error in test_cases:
        if (not error) and (password in query):
            assert password in node.query(
                f"SHOW CREATE TABLE {table_name} {show_secrets}=1"
            )
            assert password in node.query(
                f"SELECT create_table_query, engine_full FROM system.tables WHERE name = '{table_name}' "
                f"{show_secrets}=1"
            )


def test_backup_table():
    password = new_password()

    setup_queries = [
        "CREATE TABLE backup_test (x int) ENGINE = MergeTree ORDER BY x",
        "INSERT INTO backup_test SELECT * FROM numbers(10)",
    ]

    endpoints_with_credentials = [
        (
            f"S3('http://minio1:9001/root/data/backup_test_base', 'minio', '{password}')",
            f"S3('http://minio1:9001/root/data/backup_test_incremental', 'minio', '{password}')",
        )
    ]

    for query in setup_queries:
        node.query_and_get_answer_with_error(query)

    # Actually need to make two backups to have base_backup
    def make_test_case(endpoint_specs):
        # Run ASYNC so it returns the backup id
        return (
            f"BACKUP TABLE backup_test TO {endpoint_specs[0]} ASYNC",
            f"BACKUP TABLE backup_test TO {endpoint_specs[1]} SETTINGS async=1, base_backup={endpoint_specs[0]}",
        )

    test_cases = [
        make_test_case(endpoint_spec) for endpoint_spec in endpoints_with_credentials
    ]
    for base_query, inc_query in test_cases:
        node.query_and_get_answer_with_error(base_query)[0]

        inc_backup_query_output = node.query_and_get_answer_with_error(inc_query)[0]
        inc_backup_id = TSV.toMat(inc_backup_query_output)[0][0]
        names_in_system_backups_output, _ = node.query_and_get_answer_with_error(
            f"SELECT base_backup_name, name FROM system.backups where id = '{inc_backup_id}'"
        )

        base_backup_name, name = TSV.toMat(names_in_system_backups_output)[0]

        assert password not in base_backup_name
        assert password not in name


def test_create_table():
    password = new_password()

    table_engines = [
        f"MySQL('mysql80:3306', 'mysql_db', 'mysql_table', 'mysql_user', '{password}')",
        f"PostgreSQL('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '{password}')",
        f"MongoDB('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', '{password}')",
        f"S3('http://minio1:9001/root/data/test1.csv')",
        f"S3('http://minio1:9001/root/data/test2.csv', 'CSV')",
        f"S3('http://minio1:9001/root/data/test3.csv.gz', 'CSV', 'gzip')",
        f"S3('http://minio1:9001/root/data/test4.csv', 'minio', '{password}', 'CSV')",
        f"S3('http://minio1:9001/root/data/test5.csv.gz', 'minio', '{password}', 'CSV', 'gzip')",
        f"MySQL(named_collection_1, host = 'mysql80', port = 3306, database = 'mysql_db', table = 'mysql_table', user = 'mysql_user', password = '{password}')",
        f"MySQL(named_collection_2, database = 'mysql_db', host = 'mysql80', port = 3306, password = '{password}', table = 'mysql_table', user = 'mysql_user')",
        f"MySQL(named_collection_3, database = 'mysql_db', host = 'mysql80', port = 3306, table = 'mysql_table')",
        f"PostgreSQL(named_collection_4, host = 'postgres1', port = 5432, database = 'postgres_db', table = 'postgres_table', user = 'postgres_user', password = '{password}')",
        f"MongoDB(named_collection_5, host = 'mongo1', port = 5432, db = 'mongo_db', collection = 'mongo_col', user = 'mongo_user', password = '{password}')",
        f"S3(named_collection_6, url = 'http://minio1:9001/root/data/test8.csv', access_key_id = 'minio', secret_access_key = '{password}', format = 'CSV')",
        f"S3('http://minio1:9001/root/data/test9.csv.gz', 'NOSIGN', 'CSV', 'gzip')",
        f"S3('http://minio1:9001/root/data/test10.csv.gz', 'minio', '{password}')",
        (
            f"DeltaLake('http://minio1:9001/root/data/test11.csv.gz', 'minio', '{password}')",
            "DNS_ERROR",
        ),
        f"S3Queue('http://minio1:9001/root/data/', 'CSV') settings mode = 'ordered'",
        f"S3Queue('http://minio1:9001/root/data/', 'CSV', 'gzip') settings mode = 'ordered'",
        f"S3Queue('http://minio1:9001/root/data/', 'minio', '{password}', 'CSV') settings mode = 'ordered'",
        f"S3Queue('http://minio1:9001/root/data/', 'minio', '{password}', 'CSV', 'gzip') settings mode = 'ordered'",
        (
            f"Iceberg('http://minio1:9001/root/data/test11.csv.gz', 'minio', '{password}')",
            "DNS_ERROR",
        ),
    ]

    def make_test_case(i):
        table_name = f"table{i}"
        table_engine = table_engines[i]
        error = None
        if isinstance(table_engine, tuple):
            table_engine, error = table_engine
        query = f"CREATE TABLE {table_name} (x int) ENGINE = {table_engine}"
        return table_name, query, error

    # Generate test cases as a list of tuples (table_name, query, error).
    test_cases = [make_test_case(i) for i in range(len(table_engines))]

    for table_name, query, error in test_cases:
        if error:
            assert error in node.query_and_get_error(query)
        else:
            node.query(query)

    for toggle, secret in enumerate(["[HIDDEN]", password]):
        assert (
            node.query(f"SHOW CREATE TABLE table0 {show_secrets}={toggle}")
            == "CREATE TABLE default.table0\\n(\\n    `x` Int32\\n)\\n"
            "ENGINE = MySQL(\\'mysql80:3306\\', \\'mysql_db\\', "
            f"\\'mysql_table\\', \\'mysql_user\\', \\'{secret}\\')\n"
        )

        assert node.query(
            f"SELECT create_table_query, engine_full FROM system.tables WHERE name = 'table0' {show_secrets}={toggle}"
        ) == TSV(
            [
                [
                    "CREATE TABLE default.table0 (`x` Int32) ENGINE = MySQL(\\'mysql80:3306\\', \\'mysql_db\\', "
                    f"\\'mysql_table\\', \\'mysql_user\\', \\'{secret}\\')",
                    f"MySQL(\\'mysql80:3306\\', \\'mysql_db\\', \\'mysql_table\\', \\'mysql_user\\', \\'{secret}\\')",
                ],
            ]
        )

    check_logs(
        must_contain=[
            "CREATE TABLE table0 (`x` int) ENGINE = MySQL('mysql80:3306', 'mysql_db', 'mysql_table', 'mysql_user', '[HIDDEN]')",
            "CREATE TABLE table1 (`x` int) ENGINE = PostgreSQL('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '[HIDDEN]')",
            "CREATE TABLE table2 (`x` int) ENGINE = MongoDB('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', '[HIDDEN]')",
            "CREATE TABLE table3 (x int) ENGINE = S3('http://minio1:9001/root/data/test1.csv')",
            "CREATE TABLE table4 (x int) ENGINE = S3('http://minio1:9001/root/data/test2.csv', 'CSV')",
            "CREATE TABLE table5 (x int) ENGINE = S3('http://minio1:9001/root/data/test3.csv.gz', 'CSV', 'gzip')",
            "CREATE TABLE table6 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test4.csv', 'minio', '[HIDDEN]', 'CSV')",
            "CREATE TABLE table7 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test5.csv.gz', 'minio', '[HIDDEN]', 'CSV', 'gzip')",
            "CREATE TABLE table8 (`x` int) ENGINE = MySQL(named_collection_1, host = 'mysql80', port = 3306, database = 'mysql_db', `table` = 'mysql_table', user = 'mysql_user', password = '[HIDDEN]')",
            "CREATE TABLE table9 (`x` int) ENGINE = MySQL(named_collection_2, database = 'mysql_db', host = 'mysql80', port = 3306, password = '[HIDDEN]', `table` = 'mysql_table', user = 'mysql_user')",
            "CREATE TABLE table10 (x int) ENGINE = MySQL(named_collection_3, database = 'mysql_db', host = 'mysql80', port = 3306, table = 'mysql_table')",
            "CREATE TABLE table11 (`x` int) ENGINE = PostgreSQL(named_collection_4, host = 'postgres1', port = 5432, database = 'postgres_db', `table` = 'postgres_table', user = 'postgres_user', password = '[HIDDEN]')",
            "CREATE TABLE table12 (`x` int) ENGINE = MongoDB(named_collection_5, host = 'mongo1', port = 5432, db = 'mongo_db', collection = 'mongo_col', user = 'mongo_user', password = '[HIDDEN]'",
            "CREATE TABLE table13 (`x` int) ENGINE = S3(named_collection_6, url = 'http://minio1:9001/root/data/test8.csv', access_key_id = 'minio', secret_access_key = '[HIDDEN]', format = 'CSV')",
            "CREATE TABLE table14 (x int) ENGINE = S3('http://minio1:9001/root/data/test9.csv.gz', 'NOSIGN', 'CSV', 'gzip')",
            "CREATE TABLE table15 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test10.csv.gz', 'minio', '[HIDDEN]')",
            "CREATE TABLE table16 (`x` int) ENGINE = DeltaLake('http://minio1:9001/root/data/test11.csv.gz', 'minio', '[HIDDEN]')",
            "CREATE TABLE table17 (x int) ENGINE = S3Queue('http://minio1:9001/root/data/', 'CSV') settings mode = 'ordered'",
            "CREATE TABLE table18 (x int) ENGINE = S3Queue('http://minio1:9001/root/data/', 'CSV', 'gzip') settings mode = 'ordered'",
            # due to sensitive data substituion the query will be normalized, so not "settings" but "SETTINGS"
            "CREATE TABLE table19 (`x` int) ENGINE = S3Queue('http://minio1:9001/root/data/', 'minio', '[HIDDEN]', 'CSV') SETTINGS mode = 'ordered'",
            "CREATE TABLE table20 (`x` int) ENGINE = S3Queue('http://minio1:9001/root/data/', 'minio', '[HIDDEN]', 'CSV', 'gzip') SETTINGS mode = 'ordered'",
            "CREATE TABLE table21 (`x` int) ENGINE = Iceberg('http://minio1:9001/root/data/test11.csv.gz', 'minio', '[HIDDEN]')",
        ],
        must_not_contain=[password],
    )

    check_secrets_for_tables(test_cases, password)

    for table_name, query, error in test_cases:
        if not error:
            node.query(f"DROP TABLE {table_name}")


def test_create_database():
    password = new_password()

    database_engines = [
        (
            f"MySQL('localhost:3306', 'mysql_db', 'mysql_user', '{password}') SETTINGS connect_timeout=1, connection_max_tries=1",
            "ALL_CONNECTION_TRIES_FAILED",
        ),
        (
            f"MySQL(named_collection_1, host = 'localhost', port = 3306, database = 'mysql_db', user = 'mysql_user', password = '{password}') SETTINGS connect_timeout=1, connection_max_tries=1",
            "ALL_CONNECTION_TRIES_FAILED",
        ),
        f"S3('http://minio1:9001/root/data', 'minio', '{password}')",
        f"S3(named_collection_2, secret_access_key = '{password}', access_key_id = 'minio')",
        # f"PostgreSQL('localhost:5432', 'postgres_db', 'postgres_user', '{password}')",
    ]

    def make_test_case(i):
        database_name = f"database{i}"
        database_engine = database_engines[i]
        error = None
        if isinstance(database_engine, tuple):
            database_engine, error = database_engine
        query = f"CREATE DATABASE {database_name} ENGINE = {database_engine}"
        return database_name, query, error

    # Generate test cases as a list of tuples (database_name, query, error).
    test_cases = [make_test_case(i) for i in range(len(database_engines))]

    for database_name, query, error in test_cases:
        if error:
            assert error in node.query_and_get_error(query)
        else:
            node.query(query)

    check_logs(
        must_contain=[
            "CREATE DATABASE database0 ENGINE = MySQL('localhost:3306', 'mysql_db', 'mysql_user', '[HIDDEN]')",
            "CREATE DATABASE database1 ENGINE = MySQL(named_collection_1, host = 'localhost', port = 3306, database = 'mysql_db', user = 'mysql_user', password = '[HIDDEN]')",
            "CREATE DATABASE database2 ENGINE = S3('http://minio1:9001/root/data', 'minio', '[HIDDEN]')",
            "CREATE DATABASE database3 ENGINE = S3(named_collection_2, secret_access_key = '[HIDDEN]', access_key_id = 'minio')",
            # "CREATE DATABASE database4 ENGINE = PostgreSQL('localhost:5432', 'postgres_db', 'postgres_user', '[HIDDEN]')",
        ],
        must_not_contain=[password],
    )

    for database_name, query, error in test_cases:
        if not error:
            node.query(f"DROP DATABASE {database_name}")


def test_table_functions():
    password = new_password()
    azure_conn_string = cluster.env_variables["AZURITE_CONNECTION_STRING"]
    account_key_pattern = re.compile("AccountKey=.*?(;|$)")
    masked_azure_conn_string = re.sub(
        account_key_pattern, "AccountKey=[HIDDEN]\\1", azure_conn_string
    )
    azure_storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    azure_account_name = "devstoreaccount1"
    azure_account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    table_functions = [
        f"mysql('mysql80:3306', 'mysql_db', 'mysql_table', 'mysql_user', '{password}')",
        f"postgresql('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '{password}')",
        f"mongodb('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', '{password}', 'x int')",
        f"s3('http://minio1:9001/root/data/test1.csv')",
        f"s3('http://minio1:9001/root/data/test2.csv', 'CSV')",
        f"s3('http://minio1:9001/root/data/test3.csv', 'minio', '{password}')",
        f"s3('http://minio1:9001/root/data/test4.csv', 'CSV', 'x int')",
        f"s3('http://minio1:9001/root/data/test5.csv.gz', 'CSV', 'x int', 'gzip')",
        f"s3('http://minio1:9001/root/data/test6.csv', 'minio', '{password}', 'CSV')",
        f"s3('http://minio1:9001/root/data/test7.csv', 'minio', '{password}', 'CSV', 'x int')",
        f"s3('http://minio1:9001/root/data/test8.csv.gz', 'minio', '{password}', 'CSV', 'x int', 'gzip')",
        f"s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test1.csv', 'minio', '{password}')",
        f"s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test2.csv', 'CSV', 'x int')",
        f"s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test3.csv', 'minio', '{password}', 'CSV')",
        f"remote('127.{{2..11}}', default.remote_table)",
        f"remote('127.{{2..11}}', default.remote_table, rand())",
        f"remote('127.{{2..11}}', default.remote_table, 'remote_user')",
        f"remote('127.{{2..11}}', default.remote_table, 'remote_user', '{password}')",
        f"remote('127.{{2..11}}', default.remote_table, 'remote_user', rand())",
        f"remote('127.{{2..11}}', default.remote_table, 'remote_user', '{password}', rand())",
        f"remote('127.{{2..11}}', 'default.remote_table', 'remote_user', '{password}', rand())",
        f"remote('127.{{2..11}}', 'default', 'remote_table', 'remote_user', '{password}', rand())",
        f"remote('127.{{2..11}}', numbers(10), 'remote_user', '{password}', rand())",
        f"remoteSecure('127.{{2..11}}', 'default', 'remote_table', 'remote_user', '{password}')",
        f"remoteSecure('127.{{2..11}}', 'default', 'remote_table', 'remote_user', rand())",
        f"mysql(named_collection_1, host = 'mysql80', port = 3306, database = 'mysql_db', table = 'mysql_table', user = 'mysql_user', password = '{password}')",
        f"postgresql(named_collection_2, password = '{password}', host = 'postgres1', port = 5432, database = 'postgres_db', table = 'postgres_table', user = 'postgres_user')",
        f"s3(named_collection_2, url = 'http://minio1:9001/root/data/test4.csv', access_key_id = 'minio', secret_access_key = '{password}')",
        f"remote(named_collection_6, addresses_expr = '127.{{2..11}}', database = 'default', table = 'remote_table', user = 'remote_user', password = '{password}', sharding_key = rand())",
        f"remoteSecure(named_collection_6, addresses_expr = '127.{{2..11}}', database = 'default', table = 'remote_table', user = 'remote_user', password = '{password}')",
        f"s3('http://minio1:9001/root/data/test9.csv.gz', 'NOSIGN', 'CSV')",
        f"s3('http://minio1:9001/root/data/test10.csv.gz', 'minio', '{password}')",
        f"deltaLake('http://minio1:9001/root/data/test11.csv.gz', 'minio', '{password}')",
        f"azureBlobStorage('{azure_conn_string}', 'cont', 'test_simple.csv', 'CSV')",
        f"azureBlobStorage('{azure_conn_string}', 'cont', 'test_simple_1.csv', 'CSV', 'none')",
        f"azureBlobStorage('{azure_conn_string}', 'cont', 'test_simple_2.csv', 'CSV', 'none', 'auto')",
        f"azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_3.csv', '{azure_account_name}', '{azure_account_key}')",
        f"azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_4.csv', '{azure_account_name}', '{azure_account_key}', 'CSV')",
        f"azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_5.csv', '{azure_account_name}', '{azure_account_key}', 'CSV', 'none')",
        f"azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_6.csv', '{azure_account_name}', '{azure_account_key}', 'CSV', 'none', 'auto')",
        f"azureBlobStorage(named_collection_2, connection_string = '{azure_conn_string}', container = 'cont', blob_path = 'test_simple_7.csv', format = 'CSV')",
        f"azureBlobStorage(named_collection_2, storage_account_url = '{azure_storage_account_url}', container = 'cont', blob_path = 'test_simple_8.csv', account_name = '{azure_account_name}', account_key = '{azure_account_key}')",
        f"azureBlobStorageCluster('test_shard_localhost', '{azure_conn_string}', 'cont', 'test_simple_9.csv', 'CSV')",
        f"azureBlobStorageCluster('test_shard_localhost', '{azure_conn_string}', 'cont', 'test_simple_10.csv', 'CSV', 'none')",
        f"azureBlobStorageCluster('test_shard_localhost', '{azure_conn_string}', 'cont', 'test_simple_11.csv', 'CSV', 'none', 'auto')",
        f"azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_12.csv', '{azure_account_name}', '{azure_account_key}')",
        f"azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_13.csv', '{azure_account_name}', '{azure_account_key}', 'CSV')",
        f"azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_14.csv', '{azure_account_name}', '{azure_account_key}', 'CSV', 'none')",
        f"azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_15.csv', '{azure_account_name}', '{azure_account_key}', 'CSV', 'none', 'auto')",
        f"azureBlobStorageCluster('test_shard_localhost', named_collection_2, connection_string = '{azure_conn_string}', container = 'cont', blob_path = 'test_simple_16.csv', format = 'CSV')",
        f"azureBlobStorageCluster('test_shard_localhost', named_collection_2, storage_account_url = '{azure_storage_account_url}', container = 'cont', blob_path = 'test_simple_17.csv', account_name = '{azure_account_name}', account_key = '{azure_account_key}')",
        f"iceberg('http://minio1:9001/root/data/test11.csv.gz', 'minio', '{password}')",
        f"gcs('http://minio1:9001/root/data/test11.csv.gz', 'minio', '{password}')",
    ]

    def make_test_case(i):
        table_name = f"tablefunc{i}"
        table_function = table_functions[i]
        error = None
        if isinstance(table_function, tuple):
            table_function, error = table_function
        query = f"CREATE TABLE {table_name} (x int) AS {table_function}"
        return table_name, query, error

    # Generate test cases as a list of tuples (table_name, query, error).
    test_cases = [make_test_case(i) for i in range(len(table_functions))]

    for table_name, query, error in test_cases:
        node.query(query)

    for toggle, secret in enumerate(["[HIDDEN]", password]):
        assert (
            node.query(f"SHOW CREATE TABLE tablefunc0 {show_secrets}={toggle}")
            == "CREATE TABLE default.tablefunc0\\n(\\n    `x` Int32\\n) AS "
            "mysql(\\'mysql80:3306\\', \\'mysql_db\\', \\'mysql_table\\', "
            f"\\'mysql_user\\', \\'{secret}\\')\n"
        )

        assert node.query(
            "SELECT create_table_query, engine_full FROM system.tables WHERE name = 'tablefunc0' "
            f"{show_secrets}={toggle}"
        ) == TSV(
            [
                [
                    "CREATE TABLE default.tablefunc0 (`x` Int32) AS mysql(\\'mysql80:3306\\', "
                    f"\\'mysql_db\\', \\'mysql_table\\', \\'mysql_user\\', \\'{secret}\\')",
                    "",
                ],
            ]
        )

    check_logs(
        must_contain=[
            "CREATE TABLE tablefunc0 (`x` int) AS mysql('mysql80:3306', 'mysql_db', 'mysql_table', 'mysql_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc1 (`x` int) AS postgresql('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc2 (`x` int) AS mongodb('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', '[HIDDEN]', 'x int')",
            "CREATE TABLE tablefunc3 (x int) AS s3('http://minio1:9001/root/data/test1.csv')",
            "CREATE TABLE tablefunc4 (x int) AS s3('http://minio1:9001/root/data/test2.csv', 'CSV')",
            "CREATE TABLE tablefunc5 (`x` int) AS s3('http://minio1:9001/root/data/test3.csv', 'minio', '[HIDDEN]')",
            "CREATE TABLE tablefunc6 (x int) AS s3('http://minio1:9001/root/data/test4.csv', 'CSV', 'x int')",
            "CREATE TABLE tablefunc7 (x int) AS s3('http://minio1:9001/root/data/test5.csv.gz', 'CSV', 'x int', 'gzip')",
            "CREATE TABLE tablefunc8 (`x` int) AS s3('http://minio1:9001/root/data/test6.csv', 'minio', '[HIDDEN]', 'CSV')",
            "CREATE TABLE tablefunc9 (`x` int) AS s3('http://minio1:9001/root/data/test7.csv', 'minio', '[HIDDEN]', 'CSV', 'x int')",
            "CREATE TABLE tablefunc10 (`x` int) AS s3('http://minio1:9001/root/data/test8.csv.gz', 'minio', '[HIDDEN]', 'CSV', 'x int', 'gzip')",
            "CREATE TABLE tablefunc11 (`x` int) AS s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test1.csv', 'minio', '[HIDDEN]')",
            "CREATE TABLE tablefunc12 (x int) AS s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test2.csv', 'CSV', 'x int')",
            "CREATE TABLE tablefunc13 (`x` int) AS s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test3.csv', 'minio', '[HIDDEN]', 'CSV')",
            "CREATE TABLE tablefunc14 (x int) AS remote('127.{2..11}', default.remote_table)",
            "CREATE TABLE tablefunc15 (x int) AS remote('127.{2..11}', default.remote_table, rand())",
            "CREATE TABLE tablefunc16 (x int) AS remote('127.{2..11}', default.remote_table, 'remote_user')",
            "CREATE TABLE tablefunc17 (`x` int) AS remote('127.{2..11}', default.remote_table, 'remote_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc18 (x int) AS remote('127.{2..11}', default.remote_table, 'remote_user', rand())",
            "CREATE TABLE tablefunc19 (`x` int) AS remote('127.{2..11}', default.remote_table, 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc20 (`x` int) AS remote('127.{2..11}', 'default.remote_table', 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc21 (`x` int) AS remote('127.{2..11}', 'default', 'remote_table', 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc22 (`x` int) AS remote('127.{2..11}', numbers(10), 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc23 (`x` int) AS remoteSecure('127.{2..11}', 'default', 'remote_table', 'remote_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc24 (x int) AS remoteSecure('127.{2..11}', 'default', 'remote_table', 'remote_user', rand())",
            "CREATE TABLE tablefunc25 (`x` int) AS mysql(named_collection_1, host = 'mysql80', port = 3306, database = 'mysql_db', `table` = 'mysql_table', user = 'mysql_user', password = '[HIDDEN]')",
            "CREATE TABLE tablefunc26 (`x` int) AS postgresql(named_collection_2, password = '[HIDDEN]', host = 'postgres1', port = 5432, database = 'postgres_db', `table` = 'postgres_table', user = 'postgres_user')",
            "CREATE TABLE tablefunc27 (`x` int) AS s3(named_collection_2, url = 'http://minio1:9001/root/data/test4.csv', access_key_id = 'minio', secret_access_key = '[HIDDEN]')",
            "CREATE TABLE tablefunc28 (`x` int) AS remote(named_collection_6, addresses_expr = '127.{2..11}', database = 'default', `table` = 'remote_table', user = 'remote_user', password = '[HIDDEN]', sharding_key = rand())",
            "CREATE TABLE tablefunc29 (`x` int) AS remoteSecure(named_collection_6, addresses_expr = '127.{2..11}', database = 'default', `table` = 'remote_table', user = 'remote_user', password = '[HIDDEN]')",
            "CREATE TABLE tablefunc30 (x int) AS s3('http://minio1:9001/root/data/test9.csv.gz', 'NOSIGN', 'CSV')",
            "CREATE TABLE tablefunc31 (`x` int) AS s3('http://minio1:9001/root/data/test10.csv.gz', 'minio', '[HIDDEN]')",
            "CREATE TABLE tablefunc32 (`x` int) AS deltaLake('http://minio1:9001/root/data/test11.csv.gz', 'minio', '[HIDDEN]')",
            f"CREATE TABLE tablefunc33 (`x` int) AS azureBlobStorage('{masked_azure_conn_string}', 'cont', 'test_simple.csv', 'CSV')",
            f"CREATE TABLE tablefunc34 (`x` int) AS azureBlobStorage('{masked_azure_conn_string}', 'cont', 'test_simple_1.csv', 'CSV', 'none')",
            f"CREATE TABLE tablefunc35 (`x` int) AS azureBlobStorage('{masked_azure_conn_string}', 'cont', 'test_simple_2.csv', 'CSV', 'none', 'auto')",
            f"CREATE TABLE tablefunc36 (`x` int) AS azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_3.csv', '{azure_account_name}', '[HIDDEN]')",
            f"CREATE TABLE tablefunc37 (`x` int) AS azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_4.csv', '{azure_account_name}', '[HIDDEN]', 'CSV')",
            f"CREATE TABLE tablefunc38 (`x` int) AS azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_5.csv', '{azure_account_name}', '[HIDDEN]', 'CSV', 'none')",
            f"CREATE TABLE tablefunc39 (`x` int) AS azureBlobStorage('{azure_storage_account_url}', 'cont', 'test_simple_6.csv', '{azure_account_name}', '[HIDDEN]', 'CSV', 'none', 'auto')",
            f"CREATE TABLE tablefunc40 (`x` int) AS azureBlobStorage(named_collection_2, connection_string = '{masked_azure_conn_string}', container = 'cont', blob_path = 'test_simple_7.csv', format = 'CSV')",
            f"CREATE TABLE tablefunc41 (`x` int) AS azureBlobStorage(named_collection_2, storage_account_url = '{azure_storage_account_url}', container = 'cont', blob_path = 'test_simple_8.csv', account_name = '{azure_account_name}', account_key = '[HIDDEN]')",
            f"CREATE TABLE tablefunc42 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', '{masked_azure_conn_string}', 'cont', 'test_simple_9.csv', 'CSV')",
            f"CREATE TABLE tablefunc43 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', '{masked_azure_conn_string}', 'cont', 'test_simple_10.csv', 'CSV', 'none')",
            f"CREATE TABLE tablefunc44 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', '{masked_azure_conn_string}', 'cont', 'test_simple_11.csv', 'CSV', 'none', 'auto')",
            f"CREATE TABLE tablefunc45 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_12.csv', '{azure_account_name}', '[HIDDEN]')",
            f"CREATE TABLE tablefunc46 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_13.csv', '{azure_account_name}', '[HIDDEN]', 'CSV')",
            f"CREATE TABLE tablefunc47 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_14.csv', '{azure_account_name}', '[HIDDEN]', 'CSV', 'none')",
            f"CREATE TABLE tablefunc48 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', '{azure_storage_account_url}', 'cont', 'test_simple_15.csv', '{azure_account_name}', '[HIDDEN]', 'CSV', 'none', 'auto')",
            f"CREATE TABLE tablefunc49 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', named_collection_2, connection_string = '{masked_azure_conn_string}', container = 'cont', blob_path = 'test_simple_16.csv', format = 'CSV')",
            f"CREATE TABLE tablefunc50 (`x` int) AS azureBlobStorageCluster('test_shard_localhost', named_collection_2, storage_account_url = '{azure_storage_account_url}', container = 'cont', blob_path = 'test_simple_17.csv', account_name = '{azure_account_name}', account_key = '[HIDDEN]')",
            "CREATE TABLE tablefunc51 (`x` int) AS iceberg('http://minio1:9001/root/data/test11.csv.gz', 'minio', '[HIDDEN]')",
        ],
        must_not_contain=[password],
    )

    check_secrets_for_tables(test_cases, password)

    for table_name, query, error in test_cases:
        if not error:
            node.query(f"DROP TABLE {table_name}")


def test_table_function_ways_to_call():
    password = new_password()

    table_function = f"s3('http://minio1:9001/root/data/testfuncw.tsv.gz', 'minio', '{password}', 'TSV', 'x int')"

    queries = [
        "CREATE TABLE tablefuncw (`x` int) AS {}",
        "INSERT INTO FUNCTION {} SELECT * FROM numbers(10)",
        "DESCRIBE TABLE {}",
    ]

    for query in queries:
        # query_and_get_answer_with_error() is used here because we don't want to stop on error "Cannot connect to AWS".
        # We test logging here and not actual work with AWS server.
        node.query_and_get_answer_with_error(query.format(table_function))

    table_function_with_hidden_arg = "s3('http://minio1:9001/root/data/testfuncw.tsv.gz', 'minio', '[HIDDEN]', 'TSV', 'x int')"

    check_logs(
        must_contain=[
            query.format(table_function_with_hidden_arg) for query in queries
        ],
        must_not_contain=[password],
    )

    node.query("DROP TABLE tablefuncw")


def test_encryption_functions():
    plaintext = new_password()
    cipher = new_password()
    key = new_password(32)
    iv8 = new_password(8)
    iv16 = new_password(16)
    add = new_password()

    encryption_functions = [
        f"encrypt('aes-256-ofb', '{plaintext}', '{key}')",
        f"encrypt('aes-256-ofb', '{plaintext}', '{key}', '{iv16}')",
        f"encrypt('aes-256-gcm', '{plaintext}', '{key}', '{iv8}')",
        f"encrypt('aes-256-gcm', '{plaintext}', '{key}', '{iv8}', '{add}')",
        f"decrypt('aes-256-ofb', '{cipher}', '{key}', '{iv16}')",
        f"aes_encrypt_mysql('aes-256-ofb', '{plaintext}', '{key}', '{iv16}')",
        f"aes_decrypt_mysql('aes-256-ofb', '{cipher}', '{key}', '{iv16}')",
        f"tryDecrypt('aes-256-ofb', '{cipher}', '{key}', '{iv16}')",
    ]

    for encryption_function in encryption_functions:
        node.query(f"SELECT {encryption_function}")

    check_logs(
        must_contain=[
            "SELECT encrypt('aes-256-ofb', '[HIDDEN]')",
            "SELECT encrypt('aes-256-gcm', '[HIDDEN]')",
            "SELECT decrypt('aes-256-ofb', '[HIDDEN]')",
            "SELECT aes_encrypt_mysql('aes-256-ofb', '[HIDDEN]')",
            "SELECT aes_decrypt_mysql('aes-256-ofb', '[HIDDEN]')",
            "SELECT tryDecrypt('aes-256-ofb', '[HIDDEN]')",
        ],
        must_not_contain=[plaintext, cipher, key, iv8, iv16, add],
    )


def test_create_dictionary():
    password = new_password()

    node.query(
        f"CREATE DICTIONARY dict1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        f"SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'user1' TABLE 'test' PASSWORD '{password}' DB 'default')) "
        f"LIFETIME(MIN 0 MAX 10) LAYOUT(FLAT())"
    )

    for toggle, secret in enumerate(["[HIDDEN]", password]):
        assert (
            node.query(f"SHOW CREATE TABLE dict1 {show_secrets}={toggle}")
            == f"CREATE DICTIONARY default.dict1\\n(\\n    `n` int DEFAULT 0,\\n    `m` int DEFAULT 1\\n)\\nPRIMARY KEY n\\nSOURCE(CLICKHOUSE(HOST \\'localhost\\' PORT 9000 USER \\'user1\\' TABLE \\'test\\' PASSWORD \\'{secret}\\' DB \\'default\\'))\\nLIFETIME(MIN 0 MAX 10)\\nLAYOUT(FLAT())\n"
        )

        assert (
            node.query(
                f"SELECT create_table_query FROM system.tables WHERE name = 'dict1' {show_secrets}={toggle}"
            )
            == f"CREATE DICTIONARY default.dict1 (`n` int DEFAULT 0, `m` int DEFAULT 1) PRIMARY KEY n SOURCE(CLICKHOUSE(HOST \\'localhost\\' PORT 9000 USER \\'user1\\' TABLE \\'test\\' PASSWORD \\'{secret}\\' DB \\'default\\')) LIFETIME(MIN 0 MAX 10) LAYOUT(FLAT())\n"
        )

    check_logs(
        must_contain=[
            "CREATE DICTIONARY dict1 (`n` int DEFAULT 0, `m` int DEFAULT 1) PRIMARY KEY n "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'user1' TABLE 'test' PASSWORD '[HIDDEN]' DB 'default')) "
            "LIFETIME(MIN 0 MAX 10) LAYOUT(FLAT())"
        ],
        must_not_contain=[password],
    )

    node.query("DROP DICTIONARY dict1")


def test_backup_to_s3():
    node.query("CREATE TABLE temptbl (x int) ENGINE=Log")
    password = new_password()

    queries = [
        f"BACKUP TABLE temptbl TO S3('http://minio1:9001/root/data/backups/backup1', 'minio', '{password}')",
        f"RESTORE TABLE temptbl AS temptbl2 FROM S3('http://minio1:9001/root/data/backups/backup1', 'minio', '{password}')",
    ]

    for query in queries:
        # query_and_get_answer_with_error() is used here because we don't want to stop on error "Cannot connect to AWS".
        # We test logging here and not actual work with AWS server.
        node.query_and_get_answer_with_error(query)

    check_logs(
        must_contain=[
            "BACKUP TABLE temptbl TO S3('http://minio1:9001/root/data/backups/backup1', 'minio', '[HIDDEN]')",
            "RESTORE TABLE temptbl AS temptbl2 FROM S3('http://minio1:9001/root/data/backups/backup1', 'minio', '[HIDDEN]')",
        ],
        must_not_contain=[password],
    )

    node.query("DROP TABLE IF EXISTS temptbl")
    node.query("DROP TABLE IF EXISTS temptbl2")


def test_on_cluster():
    password = new_password()

    node.query(
        f"CREATE TABLE table_oncl ON CLUSTER 'test_shard_localhost' (x int) ENGINE = MySQL('mysql80:3307', 'mysql_db', 'mysql_table', 'mysql_user', '{password}')"
    )

    check_logs(
        must_contain=[
            "CREATE TABLE table_oncl ON CLUSTER test_shard_localhost (`x` int) ENGINE = MySQL('mysql80:3307', 'mysql_db', 'mysql_table', 'mysql_user', '[HIDDEN]')",
        ],
        must_not_contain=[password],
    )

    # Check logs of DDLWorker during executing of this query.
    assert node.contains_in_log(
        "DDLWorker: Processing task .*CREATE TABLE default\\.table_oncl UUID '[0-9a-fA-F-]*' (\\`x\\` Int32) ENGINE = MySQL('mysql80:3307', 'mysql_db', 'mysql_table', 'mysql_user', '\\[HIDDEN\\]')"
    )
    assert node.contains_in_log(
        "DDLWorker: Executing query: .*CREATE TABLE default\\.table_oncl UUID '[0-9a-fA-F-]*' (\\`x\\` Int32) ENGINE = MySQL('mysql80:3307', 'mysql_db', 'mysql_table', 'mysql_user', '\\[HIDDEN\\]')"
    )
    assert node.contains_in_log(
        "executeQuery: .*CREATE TABLE default\\.table_oncl UUID '[0-9a-fA-F-]*' (\\`x\\` Int32) ENGINE = MySQL('mysql80:3307', 'mysql_db', 'mysql_table', 'mysql_user', '\\[HIDDEN\\]')"
    )
    assert node.contains_in_log(
        "DDLWorker: Executed query: .*CREATE TABLE default\\.table_oncl UUID '[0-9a-fA-F-]*' (\\`x\\` Int32) ENGINE = MySQL('mysql80:3307', 'mysql_db', 'mysql_table', 'mysql_user', '\\[HIDDEN\\]')"
    )
    assert system_query_log_contains_search_pattern(
        "%CREATE TABLE default.table_oncl UUID \\'%\\' (`x` Int32) ENGINE = MySQL(\\'mysql80:3307\\', \\'mysql_db\\', \\'mysql_table\\', \\'mysql_user\\', \\'[HIDDEN]\\')"
    )

    node.query("DROP TABLE table_oncl")

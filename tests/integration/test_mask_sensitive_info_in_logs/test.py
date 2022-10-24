import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


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
        assert node.contains_in_log(str)

    for str in must_not_contain:
        assert not node.contains_in_log(str)

    for str in must_contain:
        escaped_str = str.replace("'", "\\'")
        assert (
            int(
                node.query(
                    f"SELECT COUNT() FROM system.query_log WHERE query LIKE '%{escaped_str}%'"
                ).strip()
            )
            >= 1
        )

    for str in must_not_contain:
        escaped_str = str.replace("'", "\\'")
        assert (
            int(
                node.query(
                    f"SELECT COUNT() FROM system.query_log WHERE query LIKE '%{escaped_str}%'"
                ).strip()
            )
            == 0
        )


# Passwords in CREATE/ALTER queries must be hidden in logs.
def test_create_alter_user():
    node.query("CREATE USER u1 IDENTIFIED BY 'qwe123' SETTINGS custom_a = 'a'")
    node.query("ALTER USER u1 IDENTIFIED BY '123qwe' SETTINGS custom_b = 'b'")
    node.query(
        "CREATE USER u2 IDENTIFIED WITH plaintext_password BY 'plainpasswd' SETTINGS custom_c = 'c'"
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
            "CREATE USER u1 IDENTIFIED WITH sha256_password",
            "ALTER USER u1 IDENTIFIED WITH sha256_password",
            "CREATE USER u2 IDENTIFIED WITH plaintext_password",
        ],
        must_not_contain=[
            "qwe123",
            "123qwe",
            "plainpasswd",
            "IDENTIFIED WITH sha256_password BY",
            "IDENTIFIED WITH sha256_hash BY",
            "IDENTIFIED WITH plaintext_password BY",
        ],
    )

    node.query("DROP USER u1, u2")


def test_create_table():
    table_engines = [
        "MySQL('mysql57:3306', 'mysql_db', 'mysql_table', 'mysql_user', 'qwe124')",
        "PostgreSQL('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', 'qwe124')",
        "MongoDB('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', 'qwe124')",
        "S3('http://minio1:9001/root/data/test1.csv')",
        "S3('http://minio1:9001/root/data/test2.csv', 'CSV')",
        "S3('http://minio1:9001/root/data/test3.csv.gz', 'CSV', 'gzip')",
        "S3('http://minio1:9001/root/data/test4.csv', 'minio', 'qwe124', 'CSV')",
        "S3('http://minio1:9001/root/data/test5.csv.gz', 'minio', 'qwe124', 'CSV', 'gzip')",
    ]

    for i, table_engine in enumerate(table_engines):
        node.query(f"CREATE TABLE table{i} (x int) ENGINE = {table_engine}")

    check_logs(
        must_contain=[
            "CREATE TABLE table0 (`x` int) ENGINE = MySQL('mysql57:3306', 'mysql_db', 'mysql_table', 'mysql_user', '[HIDDEN]')",
            "CREATE TABLE table1 (`x` int) ENGINE = PostgreSQL('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '[HIDDEN]')",
            "CREATE TABLE table2 (`x` int) ENGINE = MongoDB('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', '[HIDDEN]')",
            "CREATE TABLE table3 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test1.csv')",
            "CREATE TABLE table4 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test2.csv', 'CSV')",
            "CREATE TABLE table5 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test3.csv.gz', 'CSV', 'gzip')",
            "CREATE TABLE table6 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test4.csv', 'minio', '[HIDDEN]', 'CSV')",
            "CREATE TABLE table7 (`x` int) ENGINE = S3('http://minio1:9001/root/data/test5.csv.gz', 'minio', '[HIDDEN]', 'CSV', 'gzip')",
        ],
        must_not_contain=["qwe124"],
    )

    for i in range(0, len(table_engines)):
        node.query(f"DROP TABLE table{i}")


def test_create_database():
    database_engines = [
        "MySQL('mysql57:3306', 'mysql_db', 'mysql_user', 'qwe125')",
        "PostgreSQL('postgres1:5432', 'postgres_db', 'postgres_user', 'qwe125')",
    ]

    for i, database_engine in enumerate(database_engines):
        # query_and_get_answer_with_error() is used here because we don't want to stop on error "Cannot connect to MySQL server".
        # We test logging here and not actual work with MySQL server.
        node.query_and_get_answer_with_error(
            f"CREATE DATABASE database{i} ENGINE = {database_engine}"
        )

    check_logs(
        must_contain=[
            "CREATE DATABASE database0 ENGINE = MySQL('mysql57:3306', 'mysql_db', 'mysql_user', '[HIDDEN]')",
            "CREATE DATABASE database1 ENGINE = PostgreSQL('postgres1:5432', 'postgres_db', 'postgres_user', '[HIDDEN]')",
        ],
        must_not_contain=["qwe125"],
    )

    for i in range(0, len(database_engines)):
        node.query(f"DROP DATABASE IF EXISTS database{i}")


def test_table_functions():
    table_functions = [
        "mysql('mysql57:3306', 'mysql_db', 'mysql_table', 'mysql_user', 'qwe126')",
        "postgresql('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', 'qwe126')",
        "mongodb('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', 'qwe126', 'x int')",
        "s3('http://minio1:9001/root/data/test1.csv')",
        "s3('http://minio1:9001/root/data/test2.csv', 'CSV')",
        "s3('http://minio1:9001/root/data/test3.csv', 'minio', 'qwe126')",
        "s3('http://minio1:9001/root/data/test4.csv', 'CSV', 'x int')",
        "s3('http://minio1:9001/root/data/test5.csv.gz', 'CSV', 'x int', 'gzip')",
        "s3('http://minio1:9001/root/data/test6.csv', 'minio', 'qwe126', 'CSV')",
        "s3('http://minio1:9001/root/data/test7.csv', 'minio', 'qwe126', 'CSV', 'x int')",
        "s3('http://minio1:9001/root/data/test8.csv.gz', 'minio', 'qwe126', 'CSV', 'x int', 'gzip')",
        "s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test1.csv', 'minio', 'qwe126')",
        "s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test2.csv', 'CSV', 'x int')",
        "s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test3.csv', 'minio', 'qwe126', 'CSV')",
        "remote('127.{2..11}', default.remote_table)",
        "remote('127.{2..11}', default.remote_table, rand())",
        "remote('127.{2..11}', default.remote_table, 'remote_user')",
        "remote('127.{2..11}', default.remote_table, 'remote_user', 'qwe126')",
        "remote('127.{2..11}', default.remote_table, 'remote_user', rand())",
        "remote('127.{2..11}', default.remote_table, 'remote_user', 'qwe126', rand())",
        "remote('127.{2..11}', 'default.remote_table', 'remote_user', 'qwe126', rand())",
        "remote('127.{2..11}', 'default', 'remote_table', 'remote_user', 'qwe126', rand())",
        "remote('127.{2..11}', numbers(10), 'remote_user', 'qwe126', rand())",
        "remoteSecure('127.{2..11}', 'default', 'remote_table', 'remote_user', 'qwe126')",
        "remoteSecure('127.{2..11}', 'default', 'remote_table', 'remote_user', rand())",
    ]

    for i, table_function in enumerate(table_functions):
        node.query(f"CREATE TABLE tablefunc{i} (x int) AS {table_function}")

    check_logs(
        must_contain=[
            "CREATE TABLE tablefunc0 (`x` int) AS mysql('mysql57:3306', 'mysql_db', 'mysql_table', 'mysql_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc1 (`x` int) AS postgresql('postgres1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc2 (`x` int) AS mongodb('mongo1:27017', 'mongo_db', 'mongo_col', 'mongo_user', '[HIDDEN]', 'x int')",
            "CREATE TABLE tablefunc3 (`x` int) AS s3('http://minio1:9001/root/data/test1.csv')",
            "CREATE TABLE tablefunc4 (`x` int) AS s3('http://minio1:9001/root/data/test2.csv', 'CSV')",
            "CREATE TABLE tablefunc5 (`x` int) AS s3('http://minio1:9001/root/data/test3.csv', 'minio', '[HIDDEN]')",
            "CREATE TABLE tablefunc6 (`x` int) AS s3('http://minio1:9001/root/data/test4.csv', 'CSV', 'x int')",
            "CREATE TABLE tablefunc7 (`x` int) AS s3('http://minio1:9001/root/data/test5.csv.gz', 'CSV', 'x int', 'gzip')",
            "CREATE TABLE tablefunc8 (`x` int) AS s3('http://minio1:9001/root/data/test6.csv', 'minio', '[HIDDEN]', 'CSV')",
            "CREATE TABLE tablefunc9 (`x` int) AS s3('http://minio1:9001/root/data/test7.csv', 'minio', '[HIDDEN]', 'CSV', 'x int')",
            "CREATE TABLE tablefunc10 (`x` int) AS s3('http://minio1:9001/root/data/test8.csv.gz', 'minio', '[HIDDEN]', 'CSV', 'x int', 'gzip')",
            "CREATE TABLE tablefunc11 (`x` int) AS s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test1.csv', 'minio', '[HIDDEN]')",
            "CREATE TABLE tablefunc12 (`x` int) AS s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test2.csv', 'CSV', 'x int')",
            "CREATE TABLE tablefunc13 (`x` int) AS s3Cluster('test_shard_localhost', 'http://minio1:9001/root/data/test3.csv', 'minio', '[HIDDEN]', 'CSV')",
            "CREATE TABLE tablefunc14 (`x` int) AS remote('127.{2..11}', default.remote_table)",
            "CREATE TABLE tablefunc15 (`x` int) AS remote('127.{2..11}', default.remote_table, rand())",
            "CREATE TABLE tablefunc16 (`x` int) AS remote('127.{2..11}', default.remote_table, 'remote_user')",
            "CREATE TABLE tablefunc17 (`x` int) AS remote('127.{2..11}', default.remote_table, 'remote_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc18 (`x` int) AS remote('127.{2..11}', default.remote_table, 'remote_user', rand())",
            "CREATE TABLE tablefunc19 (`x` int) AS remote('127.{2..11}', default.remote_table, 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc20 (`x` int) AS remote('127.{2..11}', 'default.remote_table', 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc21 (`x` int) AS remote('127.{2..11}', 'default', 'remote_table', 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc22 (`x` int) AS remote('127.{2..11}', numbers(10), 'remote_user', '[HIDDEN]', rand())",
            "CREATE TABLE tablefunc23 (`x` int) AS remoteSecure('127.{2..11}', 'default', 'remote_table', 'remote_user', '[HIDDEN]')",
            "CREATE TABLE tablefunc24 (`x` int) AS remoteSecure('127.{2..11}', 'default', 'remote_table', 'remote_user', rand())",
        ],
        must_not_contain=["qwe126"],
    )

    for i in range(0, len(table_functions)):
        node.query(f"DROP TABLE tablefunc{i}")


def test_encryption_functions():
    encryption_functions = [
        "encrypt('aes-256-ofb', 'qwe127', 'encryptionkey_encryptionkey_encr')",
        "encrypt('aes-256-ofb', 'qwe127', 'encryptionkey_encryptionkey_encr', 'iv_iv_iv_iv_iv_i')",
        "encrypt('aes-256-gcm', 'qwe127', 'encryptionkey_encryptionkey_encr', 'iv_iv_iv')",
        "encrypt('aes-256-gcm', 'qwe127', 'encryptionkey_encryptionkey_encr', 'iv_iv_iv', 'add')",
        "decrypt('aes-256-ofb', unhex('3AC43029BF24'), 'encryptionkey_encryptionkey_encr', 'iv_iv_iv_iv_iv_i')",
        "aes_encrypt_mysql('aes-256-ofb', 'qwe127', 'encryptionkey_encryptionkey_encr', 'iv_iv_iv_iv_iv_i')",
        "aes_decrypt_mysql('aes-256-ofb', unhex('3AC43029BF24'), 'encryptionkey_encryptionkey_encr', 'iv_iv_iv_iv_iv_i')",
        "tryDecrypt('aes-256-ofb', unhex('3AC43029BF24'), 'encryptionkey_encryptionkey_encr', 'iv_iv_iv_iv_iv_i')",
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
        must_not_contain=["qwe127", "3AC43029BF24"],
    )


def test_create_dictionary():
    node.query(
        "CREATE DICTIONARY dict1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'user1' TABLE 'test' PASSWORD 'qwe128' DB 'default')) "
        "LIFETIME(MIN 0 MAX 10) LAYOUT(FLAT())"
    )

    check_logs(
        must_contain=[
            "CREATE DICTIONARY dict1 (`n` int DEFAULT 0, `m` int DEFAULT 1) PRIMARY KEY n "
            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'user1' TABLE 'test' PASSWORD '[HIDDEN]' DB 'default')) "
            "LIFETIME(MIN 0 MAX 10) LAYOUT(FLAT())"
        ],
        must_not_contain=["qwe128"],
    )

    node.query("DROP DICTIONARY dict1")


def test_backup_to_s3():
    node.query("CREATE TABLE temptbl (x int) ENGINE=Log")

    queries = [
        "BACKUP TABLE temptbl TO S3('http://minio1:9001/root/data/backups/backup1', 'minio', 'qwe129')",
        "RESTORE TABLE temptbl AS temptbl2 FROM S3('http://minio1:9001/root/data/backups/backup1', 'minio', 'qwe129')",
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
        must_not_contain=["qwe129"],
    )

    node.query("DROP TABLE IF EXISTS temptbl")
    node.query("DROP TABLE IF EXISTS temptbl2")


def test_on_cluster():
    node.query("CREATE TABLE table_oncl ON CLUSTER 'test_shard_localhost' (x int) ENGINE = MySQL('mysql57:3307', 'mysql_db', 'mysql_table', 'mysql_user', 'qwe130')")

    check_logs(
        must_contain=[
            "CREATE TABLE table_oncl ON CLUSTER test_shard_localhost (`x` int) ENGINE = MySQL('mysql57:3307', 'mysql_db', 'mysql_table', 'mysql_user', '[HIDDEN]')",
            "CREATE TABLE default.table_oncl",
        ],
        must_not_contain=["qwe130"],
    )

    node.query(f"DROP TABLE table_oncl")

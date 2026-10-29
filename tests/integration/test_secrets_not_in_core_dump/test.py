import pytest
import redis

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/secrets.xml"],
    user_configs=["configs/users.xml"],
    with_redis=True,
    env_variables={"MALLOC_CONF": "junk:free"},
)

MARKER = "nodump_marker_9c3f7a1e5d2b8604"
S3_TABLE_SECRET = "nodump_s3_table_secret_9a4e7c1d2b8f5360"
DICTIONARY_PASSWORD = "nodump_dictionary_password_5c1e8a3f7b2d9064"
SQL_NAMED_COLLECTION_PASSWORD = "nodump_sql_named_collection_password_6e2a9c4f8b1d3075"
TABLE_SETTING_DISK_KEY = "nodump_table_disk_key_1f8b2c4d6e"
DATA_LAKE_CATALOG_CREDENTIAL = "nodump_data_lake_catalog_credential_3d7f1b9e5a2c8046"
REDIS_PASSWORD = "nodump_redis_password_9e4b7c2a6f1d3085"
S3_TABLE_SESSION_TOKEN = "nodump_s3_table_session_token_4a8d2f6b9c1e3057"
MEMORY_DATABASE_TABLE_SECRET = "nodump_memory_database_table_secret_8c2e5a9f3b7d1064"
REMOTE_DATABASE_PASSWORD = "nodump_remote_database_password_1d9f3b7e5c2a8046"
URL_TABLE_HEADER = "nodump_url_table_header_6b1e4d8a2f9c3075"
ELASTICSEARCH_API_KEY = "nodump_elasticsearch_api_key_3f7a1c9e5b2d8046"
SECRETS = [
    "nodump_interserver_password_4f1c2a9e7b3d5068",
    "nodump_cluster_password_8a2e6c4d1f0b9375",
    "nodump_codec_key_c7d3e1a95b8f246",
    "nodump_named_collection_password_2b9d7e4f1a6c3580",
    "nodump_s3_disk_secret_3e8c5a2f9b7d1640",
    "nodump_disk_key_e5a1b8c3d9f24760",
    S3_TABLE_SECRET,
    DICTIONARY_PASSWORD,
    SQL_NAMED_COLLECTION_PASSWORD,
    TABLE_SETTING_DISK_KEY,
    DATA_LAKE_CATALOG_CREDENTIAL,
    REDIS_PASSWORD,
    S3_TABLE_SESSION_TOKEN,
    MEMORY_DATABASE_TABLE_SECRET,
    REMOTE_DATABASE_PASSWORD,
    URL_TABLE_HEADER,
    ELASTICSEARCH_API_KEY,
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_core_dump_has_no_secrets(started_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("core dumps of sanitizer builds are too large")

    # (1) Plant secrets where possible (both by running queries and by config).
    # (2) Force a core dump.
    # (3) Grep for the secrets, assert that none are found (except for the positive control one).

    # Positive control
    node.query(
        f"CREATE TABLE marker (key UInt64, value String) ENGINE = Memory COMMENT '{MARKER}'"
    )

    # Planting secrets
    node.query(
        "CREATE TABLE s3_table (x UInt64) "
        f"ENGINE = S3('http://localhost:1/table/data.csv', 'table_key', '{S3_TABLE_SECRET}', '{S3_TABLE_SESSION_TOKEN}', 'CSV')"
    )
    node.query(
        f"CREATE USER dictionary_user IDENTIFIED WITH sha256_password BY '{DICTIONARY_PASSWORD}'"
    )
    node.query("GRANT SELECT ON default.marker TO dictionary_user")
    node.query(
        "CREATE DICTIONARY dictionary (key UInt64, value String) PRIMARY KEY key "
        "SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'dictionary_user' "
        f"password '{DICTIONARY_PASSWORD}' db 'default' table 'marker')) "
        "LAYOUT(FLAT()) LIFETIME(0)"
    )
    node.query("SYSTEM RELOAD DICTIONARY dictionary")
    node.query(
        f"CREATE NAMED COLLECTION sql_collection AS password = '{SQL_NAMED_COLLECTION_PASSWORD}'"
    )
    node.query(
        "CREATE TABLE table_with_disk_setting (x UInt64) ENGINE = MergeTree ORDER BY x "
        "SETTINGS disk = disk(name = 'table_encrypted_disk', type = encrypted, disk = 'default', "
        f"path = 'table_encrypted_disk/', algorithm = 'aes_256_ctr', key = '{TABLE_SETTING_DISK_KEY}')"
    )
    node.query(
        "ATTACH DATABASE data_lake ENGINE = DataLakeCatalog('http://localhost:1/') "
        f"SETTINGS catalog_type = 'rest', warehouse = 'warehouse', catalog_credential = 'client_id:{DATA_LAKE_CATALOG_CREDENTIAL}'"
    )
    redis.Redis(
        host="localhost", port=started_cluster.redis_port, password="clickhouse"
    ).config_set("requirepass", REDIS_PASSWORD)
    node.query(
        "CREATE TABLE redis_table (k String, v String) "
        f"ENGINE = Redis('redis1:6379', 0, '{REDIS_PASSWORD}') PRIMARY KEY (k)"
    )
    node.query("SELECT * FROM redis_table FORMAT Null")
    node.query("CREATE DATABASE memory_database ENGINE = Memory")
    node.query(
        "CREATE TABLE memory_database.s3_table (x UInt64) "
        f"ENGINE = S3('http://localhost:1/table/data.csv', 'table_key', '{MEMORY_DATABASE_TABLE_SECRET}', 'CSV')"
    )
    node.query(
        "CREATE DATABASE remote_database "
        f"ENGINE = Remote('localhost:1', 'default', 'remote_user', '{REMOTE_DATABASE_PASSWORD}')"
    )
    node.query(
        "CREATE TABLE url_table (x UInt64) "
        f"ENGINE = URL('http://localhost:1/', 'CSV', 'none', headers('Authorization' = '{URL_TABLE_HEADER}'))"
    )
    node.query(
        "CREATE TABLE elasticsearch_table "
        f"ENGINE = Elasticsearch('http://localhost:1', 'index', api_key = '{ELASTICSEARCH_API_KEY}')",
        settings={"enable_elasticsearch": 1},
    )

    # Needed to force the configured disks to be actually created
    node.query("SELECT * FROM system.disks FORMAT Null")

    # Forcing a core dump and grepping for secrets
    pid = node.get_process_pid("clickhouse")
    node.exec_in_container(["gcore", "-o", "/tmp/core", str(pid)], user="root")
    patterns = " ".join(f"-e {needle}" for needle in [MARKER, *SECRETS])
    output = node.exec_in_container(
        ["bash", "-c", f"grep -a -o -F {patterns} /tmp/core.{pid} | sort | uniq -c"],
        user="root",
    )
    node.exec_in_container(["rm", f"/tmp/core.{pid}"], user="root")
    found = dict(reversed(line.split()) for line in output.splitlines())
    assert (
        MARKER in found
    ), "the dumpable marker is missing from the core dump, so the check is blind"
    del found[MARKER]
    assert not found, f"secrets found in the core dump: {found}"

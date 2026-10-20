import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/secrets.xml"],
    env_variables={"MALLOC_CONF": "junk:free"},
)

MARKER = "nodump_marker_9c3f7a1e5d2b8604"
S3_TABLE_SECRET = "nodump_s3_table_secret_9a4e7c1d2b8f5360"
DICTIONARY_PASSWORD = "nodump_dictionary_password_5c1e8a3f7b2d9064"
SECRETS = [
    "nodump_interserver_password_4f1c2a9e7b3d5068",
    "nodump_cluster_password_8a2e6c4d1f0b9375",
    "nodump_codec_key_c7d3e1a95b8f246",
    "nodump_named_collection_password_2b9d7e4f1a6c3580",
    "nodump_s3_disk_secret_3e8c5a2f9b7d1640",
    "nodump_disk_key_e5a1b8c3d9f24760",
    S3_TABLE_SECRET,
    DICTIONARY_PASSWORD,
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

    # positive control
    node.query(
        f"CREATE TABLE marker (key UInt64, value String) ENGINE = Memory COMMENT '{MARKER}'"
    )

    node.query(
        "CREATE TABLE s3_table (x UInt64) "
        f"ENGINE = S3('http://localhost:1/table/data.csv', 'table_key', '{S3_TABLE_SECRET}', 'CSV')"
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
    node.query("SELECT * FROM system.disks FORMAT Null")

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

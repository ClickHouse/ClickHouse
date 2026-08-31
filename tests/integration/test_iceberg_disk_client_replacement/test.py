import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/storage_conf.xml",
        "configs/s3_settings_override.xml",
    ],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_iceberg_disk_setting_does_not_replace_disk_client(started_cluster):
    node.query("DROP TABLE IF EXISTS t_mt SYNC")
    node.query(
        "CREATE TABLE t_mt (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS disk = 's3_disk_repro'"
    )

    node.query("INSERT INTO t_mt VALUES (1)")
    assert node.query("SELECT count() FROM t_mt").strip() == "1"

    node.query_and_get_error(
        "SELECT * FROM icebergS3('no_such_iceberg_table', 'Parquet', 'a Int', "
        "SETTINGS disk = 's3_disk_repro')"
    )

    node.query("INSERT INTO t_mt VALUES (2)")
    assert node.query("SELECT count() FROM t_mt").strip() == "2"

    node.query("DROP TABLE t_mt SYNC")


def test_iceberg_engine_on_disk_works(started_cluster):
    node.query("DROP TABLE IF EXISTS t_ice SYNC")
    node.query(
        "CREATE TABLE t_ice (k UInt64) ENGINE = Iceberg(path = 'iceberg_tbl') "
        "SETTINGS disk = 's3_disk_repro'"
    )

    node.query("INSERT INTO t_ice VALUES (1)", settings={"allow_insert_into_iceberg": 1})
    assert node.query("SELECT count() FROM t_ice").strip() == "1"

    node.query("DROP TABLE t_ice SYNC")


def test_disk_config_change_propagates(started_cluster):
    config_path = "/etc/clickhouse-server/config.d/storage_conf.xml"

    node.query("DROP TABLE IF EXISTS t_ice2 SYNC")
    node.query(
        "CREATE TABLE t_ice2 (k UInt64) ENGINE = Iceberg(path = 'iceberg_tbl2') "
        "SETTINGS disk = 's3_disk_repro'"
    )
    node.query("INSERT INTO t_ice2 VALUES (1)", settings={"allow_insert_into_iceberg": 1})
    assert node.query("SELECT count() FROM t_ice2").strip() == "1"

    try:
        node.replace_in_config(config_path, "ClickHouse_Minio_P@ssw0rd", "broken_secret")
        node.query("SYSTEM RELOAD CONFIG")

        error = node.query_and_get_error("SELECT count() FROM t_ice2")
        assert "S3_ERROR" in error or "Access Denied" in error or "SignatureDoesNotMatch" in error
    finally:
        node.replace_in_config(config_path, "broken_secret", "ClickHouse_Minio_P@ssw0rd")
        node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT count() FROM t_ice2").strip() == "1"

    node.query("DROP TABLE t_ice2 SYNC")

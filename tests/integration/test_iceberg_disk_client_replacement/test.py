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


def dequeued_read_requests():
    return int(
        node.query(
            "SELECT dequeued_requests FROM system.scheduler "
            "WHERE resource = 'network_read' AND path ILIKE '%/admin/%' AND type = 'fifo'"
        ).strip()
    )


def s3_clients_created():
    return int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event = 'S3Clients'"
        ).strip()
    )


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

    node.query(
        "INSERT INTO t_ice VALUES (1)", settings={"allow_insert_into_iceberg": 1}
    )
    assert node.query("SELECT count() FROM t_ice").strip() == "1"

    node.query("DROP TABLE t_ice SYNC")


def test_disk_config_change_propagates(started_cluster):
    config_path = "/etc/clickhouse-server/config.d/storage_conf.xml"

    node.query("DROP TABLE IF EXISTS t_ice2 SYNC")
    node.query(
        "CREATE TABLE t_ice2 (k UInt64) ENGINE = Iceberg(path = 'iceberg_tbl2') "
        "SETTINGS disk = 's3_disk_repro'"
    )
    node.query(
        "INSERT INTO t_ice2 VALUES (1)", settings={"allow_insert_into_iceberg": 1}
    )
    assert node.query("SELECT count() FROM t_ice2").strip() == "1"

    try:
        node.replace_in_config(
            config_path, "ClickHouse_Minio_P@ssw0rd", "broken_secret"
        )
        node.query("SYSTEM RELOAD CONFIG")

        error = node.query_and_get_error("SELECT count() FROM t_ice2")
        assert (
            "S3_ERROR" in error
            or "Access Denied" in error
            or "SignatureDoesNotMatch" in error
        )
    finally:
        node.replace_in_config(
            config_path, "broken_secret", "ClickHouse_Minio_P@ssw0rd"
        )
        node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT count() FROM t_ice2").strip() == "1"

    node.query("DROP TABLE t_ice2 SYNC")


def test_disk_config_change_propagates_through_cache_disk(started_cluster):
    config_path = "/etc/clickhouse-server/config.d/storage_conf.xml"

    node.query("DROP TABLE IF EXISTS t_ice3 SYNC")
    node.query(
        "CREATE TABLE t_ice3 (k UInt64) ENGINE = Iceberg(path = 'iceberg_tbl3') "
        "SETTINGS disk = 's3_cache_repro'"
    )
    node.query(
        "INSERT INTO t_ice3 VALUES (1)", settings={"allow_insert_into_iceberg": 1}
    )
    assert node.query("SELECT count() FROM t_ice3").strip() == "1"

    try:
        node.replace_in_config(
            config_path, "ClickHouse_Minio_P@ssw0rd", "broken_secret"
        )
        node.query("SYSTEM RELOAD CONFIG")
        # Make sure the next read cannot be served from the filesystem cache.
        node.query("SYSTEM DROP FILESYSTEM CACHE")

        # The credentials change must reach the backend storage of the table's cache-wrapped
        # object storage (the settings live in the section of the wrapped S3 disk).
        error = node.query_and_get_error("SELECT count() FROM t_ice3")
        assert (
            "S3_ERROR" in error
            or "Access Denied" in error
            or "SignatureDoesNotMatch" in error
        )
    finally:
        node.replace_in_config(
            config_path, "broken_secret", "ClickHouse_Minio_P@ssw0rd"
        )
        node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT count() FROM t_ice3").strip() == "1"

    node.query("DROP TABLE t_ice3 SYNC")


def test_client_rebuilt_only_on_client_affecting_change(started_cluster):
    config_path = "/etc/clickhouse-server/config.d/storage_conf.xml"

    node.query("DROP TABLE IF EXISTS t_ice4 SYNC")
    node.query(
        "CREATE TABLE t_ice4 (k UInt64) ENGINE = Iceberg(path = 'iceberg_tbl4') "
        "SETTINGS disk = 's3_disk_repro'"
    )
    node.query(
        "INSERT INTO t_ice4 VALUES (1)", settings={"allow_insert_into_iceberg": 1}
    )

    # Repeated queries must not rebuild the S3 client of the table.
    before = s3_clients_created()
    for _ in range(3):
        assert node.query("SELECT count() FROM t_ice4").strip() == "1"
    assert s3_clients_created() == before

    try:
        # A request setting the client is built from (marked `AFFECTS_CLIENT`):
        # exactly one rebuild on the next query, then none.
        node.replace_in_config(
            config_path, "<s3_retry_attempts>10<", "<s3_retry_attempts>11<"
        )
        node.query("SYSTEM RELOAD CONFIG")
        before = s3_clients_created()
        assert node.query("SELECT count() FROM t_ice4").strip() == "1"
        assert s3_clients_created() == before + 1
        for _ in range(3):
            assert node.query("SELECT count() FROM t_ice4").strip() == "1"
        assert s3_clients_created() == before + 1

        # A request setting the client is not built from: no rebuild.
        node.replace_in_config(
            config_path,
            "<s3_max_single_read_retries>4<",
            "<s3_max_single_read_retries>5<",
        )
        node.query("SYSTEM RELOAD CONFIG")
        before = s3_clients_created()
        for _ in range(3):
            assert node.query("SELECT count() FROM t_ice4").strip() == "1"
        assert s3_clients_created() == before
    finally:
        node.replace_in_config(
            config_path, "<s3_retry_attempts>11<", "<s3_retry_attempts>10<"
        )
        node.replace_in_config(
            config_path,
            "<s3_max_single_read_retries>5<",
            "<s3_max_single_read_retries>4<",
        )
        node.query("SYSTEM RELOAD CONFIG")

    node.query("DROP TABLE t_ice4 SYNC")


def test_disk_resource_applies_to_table(started_cluster):
    node.query("DROP TABLE IF EXISTS t_ice5 SYNC")
    node.query("DROP TABLE IF EXISTS t_ice6 SYNC")
    node.query(
        "CREATE TABLE t_ice5 (k UInt64) ENGINE = Iceberg(path = 'iceberg_tbl5') "
        "SETTINGS disk = 's3_disk_repro'"
    )
    node.query(
        "CREATE TABLE t_ice6 (k UInt64) ENGINE = Iceberg(path = 'iceberg_tbl6') "
        "SETTINGS disk = 's3_cache_repro'"
    )
    node.query(
        "INSERT INTO t_ice5 VALUES (1)", settings={"allow_insert_into_iceberg": 1}
    )
    node.query(
        "INSERT INTO t_ice6 VALUES (1)", settings={"allow_insert_into_iceberg": 1}
    )

    try:
        # The resource is created after the tables: the tables' copies of the disks' object
        # storages must follow the disks' resources, both for a plain S3 disk and a cache disk.
        node.query("""
            CREATE RESOURCE network_read (READ DISK s3_disk_repro, READ DISK s3_cache_repro);
            CREATE WORKLOAD all;
            CREATE WORKLOAD admin IN all;
            """)
        for table in ("t_ice5", "t_ice6"):
            before = dequeued_read_requests()
            assert (
                node.query(
                    f"SELECT count() FROM {table} SETTINGS workload = 'admin'"
                ).strip()
                == "1"
            )
            assert dequeued_read_requests() > before
    finally:
        node.query("""
            DROP WORKLOAD IF EXISTS admin;
            DROP WORKLOAD IF EXISTS all;
            DROP RESOURCE IF EXISTS network_read;
            """)

    node.query("DROP TABLE t_ice5 SYNC")
    node.query("DROP TABLE t_ice6 SYNC")

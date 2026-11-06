import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/storage_conf.xml"
MINIO = "http://minio1:9001/root/data"
CREDS = "'minio', 'ClickHouse_Minio_P@ssw0rd'"

TEMPLATE = f"""<clickhouse>
    <storage_configuration>
        <disks>
            <merged_s3>
                <type>s3</type>
                <endpoint>{MINIO}/merged_disk/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>
                DISK_SETTINGS
            </merged_s3>
        </disks>
    </storage_configuration>
    <allowed_disks_for_table_engines>merged_s3</allowed_disks_for_table_engines>
    <s3>S3_SETTINGS</s3>
</clickhouse>"""

MIB = 1024 * 1024


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def configure(disk="", s3="", restart=True):
    node.replace_config(
        CONFIG_PATH,
        TEMPLATE.replace("DISK_SETTINGS", disk).replace("S3_SETTINGS", s3),
    )
    # Start each case from its own configuration.
    if restart:
        node.restart_clickhouse()


def endpoint(name, path, settings):
    return f"<{name}><endpoint>{MINIO}/{path}/</endpoint>{settings}</{name}>"


def write_and_merge_on_disk(table):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS disk = 'merged_s3'"
    )
    node.query(f"INSERT INTO {table} VALUES (1)")
    node.query(f"INSERT INTO {table} VALUES (2)")
    node.query(f"OPTIMIZE TABLE {table} FINAL")
    assert node.query(f"SELECT count() FROM {table}").strip() == "2"
    node.query(f"DROP TABLE {table} SYNC")


S1_CONFIG = f"<min_upload_part_size>{5 * MIB}</min_upload_part_size>" + endpoint(
    "s1", "s1", f"<max_upload_part_size>{8 * MIB}</max_upload_part_size>"
)


def test_endpoint_section_valid_with_global_section(started_cluster):
    # Valid only with the global min 5 MiB under it (the default min is 16 MiB).
    configure(s3=S1_CONFIG)
    url = f"'{MINIO}/s1/x.tsv', {CREDS}, 'TSV', 'x UInt8'"

    node.query(
        f"INSERT INTO FUNCTION s3({url}) SETTINGS s3_truncate_on_insert = 1 SELECT 1"
    )
    assert node.query(f"SELECT x FROM s3({url})").strip() == "1"

    error = node.query_and_get_error(
        f"INSERT INTO FUNCTION s3({url}) SETTINGS s3_truncate_on_insert = 1 SELECT 1",
        settings={"s3_max_upload_part_size": 4 * MIB},
    )
    assert (
        "max_upload_part_size (4.00 MiB) can't be less than setting min_upload_part_size (5.00 MiB)"
        in error
    )


def test_disk_section_makes_endpoint_section_valid(started_cluster):
    configure(
        disk=f"<s3_min_upload_part_size>{5 * MIB}</s3_min_upload_part_size>",
        s3=endpoint(
            "d",
            "merged_disk",
            f"<max_upload_part_size>{8 * MIB}</max_upload_part_size>",
        ),
    )
    node.query("SYSTEM RELOAD CONFIG")
    write_and_merge_on_disk("t_s2")


def test_disk_reload_with_contradicting_endpoint_section_is_refused(started_cluster):
    disk = f"<s3_max_upload_part_size>{32 * MIB}</s3_max_upload_part_size>"
    # Each section is valid alone, the merged settings are not.
    configure(
        disk=disk,
        s3=endpoint(
            "d",
            "merged_disk",
            f"<min_upload_part_size>{64 * MIB}</min_upload_part_size>",
        ),
    )
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert (
        "max_upload_part_size (32.00 MiB) can't be less than setting min_upload_part_size (64.00 MiB)"
        in error
    )
    # The disk keeps the settings it was created with.
    write_and_merge_on_disk("t_s3")

    # Fixing the endpoint section is picked up by the next reload.
    configure(
        disk=disk,
        s3=endpoint(
            "d",
            "merged_disk",
            f"<min_upload_part_size>{5 * MIB}</min_upload_part_size>",
        ),
        restart=False,
    )
    node.query("SYSTEM RELOAD CONFIG")
    write_and_merge_on_disk("t_s3")

    # A contradiction brought by the endpoint section alone is refused by the same reload.
    configure(
        disk=disk,
        s3=endpoint(
            "d",
            "merged_disk",
            f"<min_upload_part_size>{64 * MIB}</min_upload_part_size>",
        ),
        restart=False,
    )
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert (
        "max_upload_part_size (32.00 MiB) can't be less than setting min_upload_part_size (64.00 MiB)"
        in error
    )


def test_data_lake_on_disk_takes_query_settings_on_top(started_cluster):
    # Disk and endpoint section give min 64 MiB > max 32 MiB, the query settings raise max to 128 MiB.
    configure(
        disk=f"<s3_min_upload_part_size>{64 * MIB}</s3_min_upload_part_size>",
        s3=endpoint(
            "d",
            "merged_disk",
            f"<max_upload_part_size>{32 * MIB}</max_upload_part_size>",
        ),
    )
    settings = {"s3_max_upload_part_size": 128 * MIB, "allow_insert_into_iceberg": 1}

    node.query("DROP TABLE IF EXISTS t_ice SYNC", settings=settings)
    node.query(
        "CREATE TABLE t_ice (x UInt8) ENGINE = Iceberg(path = 'iceberg_s4') SETTINGS disk = 'merged_s3'",
        settings=settings,
    )
    node.query("INSERT INTO t_ice VALUES (1)", settings=settings)
    assert node.query("SELECT count() FROM t_ice", settings=settings).strip() == "1"
    node.query("DROP TABLE t_ice SYNC", settings=settings)

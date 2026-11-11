import uuid

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
            OTHER_DISKS
        </disks>
    </storage_configuration>
    <allowed_disks_for_table_engines>merged_s3</allowed_disks_for_table_engines>
    <s3>S3_SETTINGS</s3>
    <!-- Only SYSTEM RELOAD CONFIG reloads the config in these tests. -->
    <config_reload_interval_ms>3600000</config_reload_interval_ms>
</clickhouse>"""

MIB = 1024 * 1024


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def configure(disk="", s3="", restart=True, other_disks=""):
    node.replace_config(
        CONFIG_PATH,
        TEMPLATE.replace("DISK_SETTINGS", disk)
        .replace("S3_SETTINGS", s3)
        .replace("OTHER_DISKS", other_disks),
    )
    # Start each case from its own configuration.
    if restart:
        node.restart_clickhouse()


def endpoint(name, path, settings):
    return f"<{name}><endpoint>{MINIO}/{path}/</endpoint>{settings}</{name}>"


def reload_config():
    query_id = str(uuid.uuid4())
    node.query("SYSTEM RELOAD CONFIG", query_id=query_id)
    return query_id


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


def test_disk_reload_with_contradicting_endpoint_section_keeps_disk_settings(
    started_cluster,
):
    disk = f"<s3_max_upload_part_size>{32 * MIB}</s3_max_upload_part_size>"
    contradicting = endpoint(
        "d", "merged_disk", f"<min_upload_part_size>{64 * MIB}</min_upload_part_size>"
    )
    not_applied = "The new S3 settings of disk merged_s3 were not applied"
    # Each section is valid alone, the merged settings are not.
    configure(disk=disk, s3=contradicting)

    # The rest of the same reload is applied.
    configure(
        disk=disk,
        s3=contradicting,
        restart=False,
        other_disks="<other_disk><path>/var/lib/clickhouse/other_disk/</path></other_disk>",
    )
    node.wait_for_log_line(f"{reload_config()}.*{not_applied}")
    assert node.contains_in_log(
        "max_upload_part_size (32.00 MiB) can't be less than setting min_upload_part_size (64.00 MiB)"
    )
    assert (
        node.query("SELECT name FROM system.disks WHERE name = 'other_disk'").strip()
        == "other_disk"
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

    # A contradiction brought by the endpoint section alone is not applied either.
    configure(disk=disk, s3=contradicting, restart=False)
    node.wait_for_log_line(f"{reload_config()}.*{not_applied}")
    write_and_merge_on_disk("t_s3")


def test_data_lake_on_disk_takes_query_settings_on_top(started_cluster):
    # The endpoint section (max 8 MiB) is invalid alone, the disk raises min to 64 MiB and the query settings max to 128 MiB.
    configure(
        disk=f"<s3_min_upload_part_size>{64 * MIB}</s3_min_upload_part_size>",
        s3=endpoint(
            "d",
            "merged_disk",
            f"<max_upload_part_size>{8 * MIB}</max_upload_part_size>",
        ),
    )
    settings = {"s3_max_upload_part_size": 128 * MIB, "allow_insert_into_iceberg": 1}

    node.query("DROP TABLE IF EXISTS t_ice SYNC", settings=settings)
    node.query(
        "CREATE TABLE t_ice (x UInt8) ENGINE = Iceberg(path = 'iceberg_s4') SETTINGS disk = 'merged_s3'",
        settings=settings,
    )
    # Without the query override, the merged disk and endpoint settings are rejected.
    error = node.query_and_get_error(
        "INSERT INTO t_ice VALUES (1)", settings={"allow_insert_into_iceberg": 1}
    )
    assert (
        "max_upload_part_size (8.00 MiB) can't be less than setting min_upload_part_size (64.00 MiB)"
        in error
    )
    node.query("INSERT INTO t_ice VALUES (1)", settings=settings)
    assert node.query("SELECT count() FROM t_ice", settings=settings).strip() == "1"
    node.query("DROP TABLE t_ice SYNC", settings=settings)


def test_query_settings_do_not_reach_other_endpoints(started_cluster):
    # The restart leaves the endpoint settings to be built by the first S3 query below.
    configure(
        s3=endpoint("a", "a", f"<min_upload_part_size>{5 * MIB}</min_upload_part_size>")
        + endpoint("b", "b", "")
    )
    url_a = f"'{MINIO}/a/x.tsv', {CREDS}, 'TSV', 'x UInt8'"
    url_b = f"'{MINIO}/b/x.tsv', {CREDS}, 'TSV', 'x UInt8'"

    node.query(
        f"INSERT INTO FUNCTION s3({url_a}) SETTINGS s3_truncate_on_insert = 1 SELECT 1",
        settings={"s3_max_upload_part_size": 8 * MIB},
    )

    # Another session with the default settings (min 16 MiB) does not get the 8 MiB maximum of that query.
    node.query(
        f"INSERT INTO FUNCTION s3({url_b}) SETTINGS s3_truncate_on_insert = 1 SELECT 2"
    )
    assert node.query(f"SELECT x FROM s3({url_b})").strip() == "2"


def test_table_does_not_keep_rejected_query_settings(started_cluster):
    configure(
        s3=endpoint("p", "p", f"<min_upload_part_size>{5 * MIB}</min_upload_part_size>")
    )
    node.query("DROP TABLE IF EXISTS t_p SYNC")
    node.query(
        f"CREATE TABLE t_p (x UInt8) ENGINE = S3('{MINIO}/p/{uuid.uuid4()}.tsv', {CREDS}, 'TSV')"
    )
    insert = "INSERT INTO t_p SETTINGS s3_truncate_on_insert = 1 VALUES (1)"
    error = node.query_and_get_error(
        insert, settings={"s3_max_upload_part_size": 4 * MIB}
    )
    assert "can't be less than setting min_upload_part_size" in error

    # The next query with the default settings is not affected.
    node.query(insert)
    assert node.query("SELECT x FROM t_p").strip() == "1"
    node.query("DROP TABLE t_p SYNC")


def test_background_write_checks_endpoint_settings(started_cluster):
    configure()
    node.query("DROP TABLE IF EXISTS t_buf SYNC")
    node.query("DROP TABLE IF EXISTS t_bg SYNC")
    # Unpartitioned: the sink opens its write buffer before the insert gets a query thread group.
    node.query(
        f"CREATE TABLE t_bg (s String) ENGINE = S3('{MINIO}/bg/{uuid.uuid4()}.bin', {CREDS}, 'RawBLOB')"
    )
    node.query(
        "CREATE TABLE t_buf (s String) ENGINE = Buffer(default, t_bg, 1, 1, 2, 1000000000, 1000000000, 1000000000, 1000000000)"
    )

    # Invalid in any combination, and a Buffer flush applies no query settings on top.
    configure(
        s3=endpoint(
            "bg",
            "bg",
            "<upload_part_size_multiply_parts_count_threshold>0</upload_part_size_multiply_parts_count_threshold>",
        ),
        restart=False,
    )
    node.query("SYSTEM RELOAD CONFIG")
    node.query("INSERT INTO t_buf VALUES ('x')")
    node.wait_for_log_line(
        "backgroundFlush.*upload_part_size_multiply_parts_count_threshold cannot be zero"
    )
    assert node.query("SELECT 1").strip() == "1"

    node.query("DROP TABLE t_buf SYNC")
    node.query("DROP TABLE t_bg SYNC")

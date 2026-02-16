import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("single_file", [True, False])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_timezone(started_cluster_iceberg_no_spark, format_version, single_file, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME_PREFIX = "test_timezone_" + storage_type + "_" + get_uuid_str()

    TABLE_NAME_ICEBERG = f"{TABLE_NAME_PREFIX}_iceberg"
    TABLE_NAME_ICEBERG_VIEW = f"{TABLE_NAME_PREFIX}_iceberg_view"
    TABLE_NAME_ICEBERG_VIEW_TZ = f"{TABLE_NAME_PREFIX}_iceberg_view_tz"
    TABLE_NAME_ICEBERG_DIST = f"{TABLE_NAME_PREFIX}_iceberg_dist"
    TABLE_NAME_MT = f"{TABLE_NAME_PREFIX}_mt"
    TABLE_NAME_ICEBERG_VIEW_64 = f"{TABLE_NAME_PREFIX}_iceberg_view_64"
    TABLE_NAME_ICEBERG_DIST_64 = f"{TABLE_NAME_PREFIX}_iceberg_dist_64"
    TABLE_NAME_MT_64 = f"{TABLE_NAME_PREFIX}_mt_64"

    settings = {'session_timezone': 'Asia/Istanbul'}

    create_iceberg_table(storage_type,
                        instance,
                        TABLE_NAME_ICEBERG,
                        started_cluster_iceberg_no_spark,
                        schema="(key Int32, value String, time DateTime)",
                        format_version=format_version,
                        )
    if single_file:
        instance.query(f"""
            INSERT INTO {TABLE_NAME_ICEBERG} VALUES
                (1, '1', '2025-01-01 01:00:00'),
                (1, '2', '2025-01-01 02:00:00'),
                (1, '3', '2025-01-01 03:00:00')
        """,
        settings={"allow_experimental_insert_into_iceberg": 1})
    else:
        instance.query(f"""
            INSERT INTO {TABLE_NAME_ICEBERG} VALUES
                (1, '1', '2025-01-01 01:00:00')
        """,
        settings={"allow_experimental_insert_into_iceberg": 1})
        instance.query(f"""
            INSERT INTO {TABLE_NAME_ICEBERG} VALUES
                (1, '2', '2025-01-01 02:00:00')
        """,
        settings={"allow_experimental_insert_into_iceberg": 1})
        instance.query(f"""
            INSERT INTO {TABLE_NAME_ICEBERG} VALUES
                (1, '3', '2025-01-01 03:00:00')
        """,
        settings={"allow_experimental_insert_into_iceberg": 1})

    instance.query(f"""
        CREATE TABLE {TABLE_NAME_MT} (key Int32, value String, time DateTime)
            ENGINE = MergeTree()
            ORDER BY key
            PARTITION BY key
    """)
    instance.query(f"""
        CREATE VIEW {TABLE_NAME_ICEBERG_VIEW} (key Int32, value String, time DateTime)
            AS SELECT * FROM {TABLE_NAME_ICEBERG}
    """)
    instance.query(f"""
        CREATE VIEW {TABLE_NAME_ICEBERG_VIEW_TZ} (key Int32, value String, time DateTime)
            AS SELECT * FROM {TABLE_NAME_ICEBERG}
    """, settings=settings)
    instance.query(f"""
        CREATE TABLE {TABLE_NAME_ICEBERG_DIST} (key Int32, value String, time DateTime)
            ENGINE = Distributed('cluster_single_node', default, {TABLE_NAME_ICEBERG})
    """)
    instance.query(f"""
        CREATE TABLE {TABLE_NAME_MT_64} (key Int32, value String, time DateTime64(6))
            ENGINE = MergeTree()
            ORDER BY key
            PARTITION BY key
    """)
    instance.query(f"""
        CREATE VIEW {TABLE_NAME_ICEBERG_VIEW_64} (key Int32, value String, time DateTime64(6))
            AS SELECT * FROM {TABLE_NAME_ICEBERG}
    """, settings=settings)
    instance.query(f"""
        CREATE TABLE {TABLE_NAME_ICEBERG_DIST_64} (key Int32, value String, time DateTime64(6))
            ENGINE = Distributed('cluster_single_node', default, {TABLE_NAME_ICEBERG})
    """)

    instance.query(f"INSERT INTO {TABLE_NAME_MT} SELECT * FROM {TABLE_NAME_ICEBERG}")
    instance.query(f"INSERT INTO {TABLE_NAME_MT_64} SELECT * FROM {TABLE_NAME_ICEBERG}")

    expected_utc_result = "1\t2025-01-01 01:00:00\n2\t2025-01-01 02:00:00\n3\t2025-01-01 03:00:00"
    expected_cond_utc_result = "2\t2025-01-01 02:00:00\n3\t2025-01-01 03:00:00"
    expected_tz_result = "1\t2025-01-01 04:00:00\n2\t2025-01-01 05:00:00\n3\t2025-01-01 06:00:00"
    expected_cond_tz_result = "2\t2025-01-01 05:00:00\n3\t2025-01-01 06:00:00"

    expected_utc_result_64 = "1\t2025-01-01 01:00:00.000000\n2\t2025-01-01 02:00:00.000000\n3\t2025-01-01 03:00:00.000000"
    expected_cond_utc_result_64 = "2\t2025-01-01 02:00:00.000000\n3\t2025-01-01 03:00:00.000000"
    expected_tz_result_64 = "1\t2025-01-01 04:00:00.000000\n2\t2025-01-01 05:00:00.000000\n3\t2025-01-01 06:00:00.000000"
    expected_cond_tz_result_64 = "2\t2025-01-01 05:00:00.000000\n3\t2025-01-01 06:00:00.000000"

    def check_utc():
        result_iceberg = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG} ORDER BY time").strip()
        result_mt = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT} ORDER BY time").strip()
        result_view = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW} ORDER BY time").strip()
        result_view_tz = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_TZ} ORDER BY time").strip()
        result_dist = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST} ORDER BY time").strip()
        result_mt_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT_64} ORDER BY time").strip()
        result_view_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_64} ORDER BY time").strip()
        result_dist_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST_64} ORDER BY time").strip()
        assert result_iceberg == expected_utc_result_64
        assert result_mt == expected_utc_result
        assert result_view == expected_utc_result
        assert result_view_tz == expected_utc_result
        assert result_dist == expected_utc_result
        assert result_mt_64 == expected_utc_result_64
        assert result_view_64 == expected_utc_result_64
        assert result_dist_64 == expected_utc_result_64

    def check_utc_cond():
        # Check with condition on time
        result_iceberg = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        result_mt = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        result_view = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        result_view_tz = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_TZ} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        result_dist = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        result_mt_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT_64} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        result_view_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_64} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        result_dist_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST_64} WHERE time >= '2025-01-01 02:00:00' ORDER BY time").strip()
        assert result_iceberg == expected_cond_utc_result_64
        assert result_mt == expected_cond_utc_result
        assert result_view == expected_cond_utc_result
        assert result_view_tz == expected_cond_utc_result # Failed before restart, 3 rows
        assert result_dist == expected_cond_utc_result
        assert result_mt_64 == expected_cond_utc_result_64
        assert result_view_64 == expected_cond_utc_result_64
        assert result_dist_64 == expected_cond_utc_result_64

    def check_tz():
        # Check with session_time
        result_iceberg = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG} ORDER BY time", settings=settings).strip()
        result_mt = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT} ORDER BY time", settings=settings).strip()
        result_view = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW} ORDER BY time", settings=settings).strip()
        result_view_tz = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_TZ} ORDER BY time", settings=settings).strip()
        result_dist = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST} ORDER BY time", settings=settings).strip()
        result_mt_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT_64} ORDER BY time", settings=settings).strip()
        result_view_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_64} ORDER BY time", settings=settings).strip()
        result_dist_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST_64} ORDER BY time", settings=settings).strip()
        assert result_iceberg == expected_tz_result_64
        assert result_mt == expected_tz_result
        assert result_view == expected_tz_result
        assert result_view_tz == expected_tz_result
        assert result_dist == expected_tz_result
        assert result_mt_64 == expected_tz_result_64
        assert result_view_64 == expected_tz_result_64
        assert result_dist_64 == expected_tz_result_64

    def check_tz_cond():
        # Check with condition on time and session_time
        result_iceberg = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        result_mt = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        result_view = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        result_view_tz = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_TZ} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        result_dist = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        result_mt_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_MT_64} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        result_view_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_VIEW_64} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        result_dist_64 = instance.query(f"SELECT value,time FROM {TABLE_NAME_ICEBERG_DIST_64} WHERE time >= '2025-01-01 02:00:00' ORDER BY time", settings=settings).strip()
        assert result_iceberg == expected_cond_tz_result_64 # Failed, 3 rows
        assert result_mt == expected_cond_tz_result
        assert result_view == expected_cond_tz_result
        assert result_view_tz == expected_cond_tz_result # Failed before restart, 3 rows
        assert result_dist == expected_cond_tz_result # Failed, 3 rows
        assert result_mt_64 == expected_cond_tz_result_64
        assert result_view_64 == expected_cond_tz_result_64 # Failed, 3 rows
        assert result_dist_64 == expected_cond_tz_result_64 # Failed, 3 rows

    check_utc()
    check_utc_cond()
    check_tz()
    check_tz_cond()

    # Restart to clean permament variables
    instance.restart_clickhouse()

    # Check in reversed order, with custom timezone first
    check_tz()
    check_tz_cond()
    check_utc()
    check_utc_cond()

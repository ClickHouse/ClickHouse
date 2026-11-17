import glob
import os
import re

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_uuid_str,
)

BERLIN = "iceberg_timezone_for_timestamptz = 'Europe/Berlin'"


def test_timestamptz_timezone(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    table_name = "test_timestamptz_timezone_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    previous_spark_timezone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
    try:
        spark.sql(f"CREATE TABLE {table_name} (id INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (days(ts))")
        spark.sql(
            f"INSERT INTO {table_name} VALUES (1, TIMESTAMP '2024-01-02 08:30:00'), (2, TIMESTAMP '2024-01-02 21:00:00')"
        )

        default_upload_directory(started_cluster_iceberg_with_spark, storage_type, f"{table_path}/", f"{table_path}/")
        create_iceberg_table(
            storage_type, instance, table_name, started_cluster_iceberg_with_spark, additional_settings=[BERLIN]
        )
        utc_table_function = f"icebergLocal(local, path = '{table_path}', format = Parquet)"

        assert (
            instance.query(f"SELECT toTypeName(ts) FROM {utc_table_function} LIMIT 1 FORMAT TSVRaw")
            == "Nullable(DateTime64(6, 'UTC'))\n"
        )
        assert (
            instance.query(f"SELECT toTypeName(ts) FROM {table_name} LIMIT 1 FORMAT TSVRaw")
            == "Nullable(DateTime64(6, 'Europe/Berlin'))\n"
        )
        assert (
            instance.query(f"SELECT id, ts FROM {table_name} ORDER BY id")
            == "1\t2024-01-02 00:30:00.000000\n2\t2024-01-02 13:00:00.000000\n"
        )

        filter_condition = "WHERE ts >= '2024-01-02 00:00:00'"
        assert instance.query(f"SELECT groupArray(id) FROM (SELECT id FROM {utc_table_function} {filter_condition} ORDER BY id)") == "[2]\n"
        assert instance.query(f"SELECT groupArray(id) FROM (SELECT id FROM {table_name} {filter_condition} ORDER BY id)") == "[1,2]\n"
        assert instance.query(f"SELECT count() FROM {table_name} WHERE ts = '2024-01-02 00:30:00'") == "1\n"

        instance.query(
            f"INSERT INTO {table_name} VALUES (3, '2024-01-03 00:30:00')",
            settings={"allow_insert_into_iceberg": 1},
        )
        assert (
            instance.query(f"SELECT ts FROM {utc_table_function} WHERE id = 3")
            == "2024-01-02 23:30:00.000000\n"
        )

        default_download_directory(started_cluster_iceberg_with_spark, storage_type, f"{table_path}/", f"{table_path}/")
        last_version = max(
            int(re.match(r"v(\d+)\.metadata\.json$", os.path.basename(path)).group(1))
            for path in glob.glob(f"{table_path}/metadata/v*.metadata.json")
        )
        with open(f"{table_path}/metadata/version-hint.text", "w") as f:
            f.write(str(last_version))

        spark.sql(f"REFRESH TABLE {table_name}")
        rows = spark.sql(
            f"SELECT id, CAST(ts AS STRING) AS ts FROM {table_name} ORDER BY id"
        ).collect()
        assert [(row.id, row.ts) for row in rows] == [
            (1, "2024-01-02 08:30:00"),
            (2, "2024-01-02 21:00:00"),
            (3, "2024-01-03 08:30:00"),
        ]
        partitions = spark.sql(
            f"SELECT CAST(partition.ts_day AS STRING) AS day, record_count FROM default.{table_name}.files ORDER BY day"
        ).collect()
        assert sorted((row.day, row.record_count) for row in partitions) == [
            ("2024-01-01", 1),
            ("2024-01-02", 1),
            ("2024-01-02", 1),
        ]
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_spark_timezone)

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str,
    default_download_directory
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_with_partitioned_table(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_with_partitioned_table_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                id INT,
                name STRING,
                value DECIMAL(10, 2),
                created_at DATE,
                event_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (bucket(3, id), bucket(2, name), bucket(5, created_at), bucket(3, event_time))
            OPTIONS('format-version'='{format_version}')
        """
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    instance.query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, 'Alice', 10.50, DATE '2024-01-20', TIMESTAMP '2024-01-20 10:00:00'),
        (2, 'Bob', 20.00, DATE '2024-01-21', TIMESTAMP '2024-01-21 11:00:00'),
        (3, 'Charlie', 30.50, DATE '2024-01-22', TIMESTAMP '2024-01-22 12:00:00'),
        (4, 'Diana', 40.00, DATE '2024-01-23', TIMESTAMP '2024-01-23 13:00:00'),
        (5, 'Eve', 50.50, DATE '2024-01-24', TIMESTAMP '2024-01-24 14:00:00');
        """,
        settings={"allow_insert_into_iceberg": 1}
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n2\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n3\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n4\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n5\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n'

    instance.query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (10, 'Alice', 10.50, DATE '2024-01-20', TIMESTAMP '2024-01-20 10:00:00'),
        (20, 'Bob', 20.00, DATE '2024-01-21', TIMESTAMP '2024-01-21 11:00:00'),
        (30, 'Charlie', 30.50, DATE '2024-01-22', TIMESTAMP '2024-01-22 12:00:00'),
        (40, 'Diana', 40.00, DATE '2024-01-23', TIMESTAMP '2024-01-23 13:00:00'),
        (50, 'Eve', 50.50, DATE '2024-01-24', TIMESTAMP '2024-01-24 14:00:00');
        """,
        settings={"allow_insert_into_iceberg": 1}
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n2\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n3\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n4\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n5\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n10\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n20\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n30\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n40\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n50\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n'

    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"3")

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 10


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_with_partitioned_table_count_partitions(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    TABLE_NAME = "test_writes_with_partitioned_table_count_partitions_" + storage_type + "_" + get_uuid_str()
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(y String)", partition_by="(icebergBucket(16, y))", format_version=format_version)
    values = ""
    num_rows = 1000
    target = []
    for i in range(num_rows):
        values += f"({i})"
        target.append(str(i))
        if i != num_rows - 1:
            values += ","
    target = '\n'.join(sorted(target))
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES {values};", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == target + "\n"

    files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    num_pq_files = 0
    for file in files:
        if file[-7:] == 'parquet':
            num_pq_files += 1

    assert num_pq_files == 16

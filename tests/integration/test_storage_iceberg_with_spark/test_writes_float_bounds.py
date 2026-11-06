from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)


def test_writes_float_bounds_read_by_spark(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    table_name = "test_writes_float_bounds_" + get_uuid_str()
    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(f32 Nullable(Float32), f64 Nullable(Float64))",
        2,
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (1.5, 1.5), (nan, nan), (0.0, 0.0), (-0.0, -0.0), (NULL, NULL)",
        settings={"allow_insert_into_iceberg": 1},
    )

    default_download_directory(started_cluster_iceberg_with_spark, storage_type, f"{path}/", f"{path}/")

    assert spark.read.format("iceberg").load(path).count() == 5

    # The bounds are decoded by Iceberg Java (`readable_metrics`), not by ClickHouse, so this checks
    # their binary encoding, e.g. that a `Float32` bound is 4 bytes.
    files = spark.read.format("iceberg").load(f"{path}#files").select(
        "null_value_counts", "nan_value_counts", "readable_metrics"
    ).collect()
    assert len(files) == 1
    data_file = files[0]
    assert data_file.null_value_counts == {1: 1, 2: 1}
    assert data_file.nan_value_counts == {1: 1, 2: 1}

    for column in ("f32", "f64"):
        metrics = data_file.readable_metrics[column]
        # NaN is excluded from the bounds, and -0.0 sorts before +0.0.
        assert str(metrics.lower_bound) == "-0.0", column
        assert metrics.upper_bound == 1.5, column

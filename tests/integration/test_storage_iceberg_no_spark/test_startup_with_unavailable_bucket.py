from helpers.iceberg_utils import create_iceberg_table, get_uuid_str


# Starting the server must not read the table metadata from the object storage. When it did, a
# table whose bucket had gone away made the start spend the S3 client's whole retry budget before
# the table was attached, which kept a service in "Starting" for tens of minutes.
# https://github.com/ClickHouse/support-escalation/issues/8579
def test_startup_with_unavailable_bucket(started_cluster_iceberg_no_spark):
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = "test_startup_with_unavailable_bucket_" + get_uuid_str()

    create_iceberg_table("s3", instance, table_name, cluster, "(x Int32)")
    instance.query(f"INSERT INTO {table_name} VALUES (1)")

    with cluster.pause_container("minio1"):
        instance.restart_clickhouse()

        # The table is listed, with unknown totals rather than a failure.
        assert (
            instance.query(
                f"SELECT total_rows IS NULL, total_bytes IS NULL FROM system.tables "
                f"WHERE database = currentDatabase() AND name = '{table_name}'"
            )
            == "1\t1\n"
        )

    assert instance.query(f"SELECT * FROM {table_name}") == "1\n"

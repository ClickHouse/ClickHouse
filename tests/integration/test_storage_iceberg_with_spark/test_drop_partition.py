import os
import re

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    execute_spark_query_general,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_with_evolved_spec_is_rejected(started_cluster_iceberg_with_spark, storage_type):
    """`ALTER TABLE ... DROP PARTITION` must refuse to operate on Iceberg
    tables whose partition spec has evolved (more than one entry in
    `partition-specs`). Manifests written under the old spec encode the
    partition tuple under a different transform set, and silently rewriting
    them against the new spec would produce a corrupt manifest. The executor
    rejects the operation with `NOT_IMPLEMENTED` until per-manifest spec
    resolution lands."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = f"test_drop_partition_evolved_spec_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, table_name, query)

    # Original table with single partition spec on `tag`.
    spark_query(
        f"""
            CREATE TABLE {table_name} (tag INT, k STRING, v INT)
            USING iceberg
            PARTITIONED BY (identity(tag))
            OPTIONS('format-version'='2')
        """)
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'a', 10), (2, 'b', 20), (1, 'c', 30)")

    # Evolve the partition spec by adding a second field.
    spark_query(f"ALTER TABLE {table_name} ADD PARTITION FIELD identity(k)")
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'd', 40), (3, 'e', 50)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )

    create_iceberg_table(storage_type, instance, table_name, started_cluster_iceberg_with_spark)

    # Sanity check the read side still works.
    assert instance.query(f"SELECT count() FROM {table_name}") == "5\n"

    # DROP PARTITION must refuse because the table now carries two partition specs.
    error = instance.query_and_get_error(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NOT_IMPLEMENTED" in error, error
    assert "evolved partition specs" in error, error

    # And data is untouched.
    assert instance.query(f"SELECT count() FROM {table_name}") == "5\n"


@pytest.mark.xfail(
    reason=
    "DROP PARTITION on a Spark-written manifest re-emits survivor entries "
    "without a `data_sequence_number`, which violates Iceberg v2 (entries "
    "with status=EXISTING must carry the sequence_number that first added "
    "the file). The follow-up SELECT raises ICEBERG_SPECIFICATION_VIOLATION "
    "from ManifestFileIterator. See the comment in `generateExistingManifestFile` "
    "for the symmetric snapshot_id handling that already refuses this case.",
    strict=True,
)
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_on_spark_table_round_trip(started_cluster_iceberg_with_spark, storage_type):
    """Happy path: a Spark-created partitioned Iceberg table is modified by
    ClickHouse with `ALTER TABLE ... DROP PARTITION`, the resulting metadata
    is shipped back to Spark, and Spark continues to read and write the table
    normally. Verifies the data files / manifests / metadata.json that
    ClickHouse emits stay compatible with the canonical Iceberg reader."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = f"test_drop_partition_round_trip_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, table_name, query)

    spark_query(
        f"""
            CREATE TABLE {table_name} (tag INT, k STRING, v INT)
            USING iceberg
            PARTITIONED BY (identity(tag))
            OPTIONS('format-version'='2')
        """)
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'a', 10), (1, 'b', 11), (2, 'c', 20), (3, 'd', 30)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )

    create_iceberg_table(storage_type, instance, table_name, started_cluster_iceberg_with_spark)
    assert instance.query(f"SELECT count() FROM {table_name}") == "4\n"

    # ClickHouse drops the `tag = 1` partition (two rows).
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT count() FROM {table_name}") == "2\n"
    assert (
        instance.query(f"SELECT tag, k, v FROM {table_name} ORDER BY tag, k, v")
        == "2\tc\t20\n3\td\t30\n"
    )

    # The DELETE snapshot must be visible in iceberg_history with the right counts.
    history = instance.query(
        f"""
            SELECT operation, summary['removed-data-files'], summary['deleted-records'],
                   summary['total-data-files'], summary['total-records']
            FROM system.iceberg_history
            WHERE table = '{table_name}'
            ORDER BY made_current_at DESC
            LIMIT 1
        """
    ).strip()
    assert history == "DELETE\t1\t2\t2\t2", history

    # Ship CH's writes back to the local Spark warehouse so Spark sees them.
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, local_dir, local_dir,
    )

    # Spark's Hadoop catalog reads the latest metadata via version-hint.text;
    # point it at the file CH just wrote. Iceberg metadata files are named
    # `v<N>.metadata.json` or `v<N>-<uuid>.metadata.json`.
    metadata_dir = os.path.join(local_dir, "metadata")
    versions = []
    for name in os.listdir(metadata_dir):
        m = re.match(r"v(\d+)(?:[-.]).*\.metadata\.json$", name)
        if m:
            versions.append(int(m.group(1)))
    assert versions, "ClickHouse should have written a new metadata.json"
    with open(os.path.join(metadata_dir, "version-hint.text"), "wb") as f:
        f.write(str(max(versions)).encode())

    rows = spark.read.format("iceberg").load(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    ).collect()
    assert sorted((r["tag"], r["k"], r["v"]) for r in rows) == [(2, "c", 20), (3, "d", 30)]

    # Spark can still insert into the table after CH's DROP PARTITION.
    spark_query(f"INSERT INTO {table_name} VALUES (4, 'e', 40)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )
    assert instance.query(f"SELECT count() FROM {table_name}") == "3\n"
    assert (
        instance.query(f"SELECT tag, k, v FROM {table_name} ORDER BY tag, k, v")
        == "2\tc\t20\n3\td\t30\n4\te\t40\n"
    )

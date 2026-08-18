import uuid

import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
)


def get_array(query_result: str):
    return sorted([int(x) for x in query_result.strip().split("\n") if x])


def upload_table(cluster, storage_type, table_name):
    default_upload_directory(
        cluster,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )


def _strip_file_uri_scheme(path):
    """Return a bare absolute path ClickHouse can remap via IcebergPathResolver.

    Hadoop `Path.toString()` yields `file:/...` / `file:///...`. For s3/azure
    table functions, those URIs can be resolved onto a secondary
    LocalObjectStorage (ClickHouse node local disk) instead of the uploaded
    object-storage tree. A scheme-less absolute path makes
    `tryResolveObjectStorageForPath` return nullopt so `IcebergPathResolver`
    remaps onto the table's base storage — matching how this harness reads
    Spark data files after `upload_table`.
    """
    if path.startswith("file://"):
        return path[len("file://") :]
    if path.startswith("file:"):
        return path[len("file:") :]
    return path


def add_equality_deletes_by_id(spark, table_name, ids):
    """Commit an Iceberg equality-delete file for the given `id` values.

    Spark SQL DELETE on v3 only writes deletion vectors, so equality deletes are
    produced by writing a Parquet file with Spark (correct long boxing) and
    registering it via Iceberg RowDelta.
    """
    jvm = spark._jvm
    ice = jvm.org.apache.iceberg
    table = ice.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"spark_catalog.default.{table_name}"
    )

    id_field_id = int(table.schema().findField("id").fieldId())
    staging_dir = table.locationProvider().newDataLocation(
        f"eq-delete-staging-{uuid.uuid4()}"
    )
    final_uri = table.locationProvider().newDataLocation(
        f"eq-delete-{uuid.uuid4()}.parquet"
    )

    # Spark DataFrame write keeps BIGINT as Long — avoids py4j Integer boxing.
    # coalesce(1) still writes a directory; flatten to a single Iceberg data file.
    spark.createDataFrame([(int(v),) for v in ids], "id: long").coalesce(1).write.mode(
        "overwrite"
    ).parquet(staging_dir)

    staging_path = jvm.org.apache.hadoop.fs.Path(staging_dir)
    fs = staging_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    part_hadoop_path = None
    part_size = 0
    for status in fs.listStatus(staging_path):
        name = status.getPath().getName()
        if name.startswith("part-") and name.endswith(".parquet"):
            part_hadoop_path = status.getPath()
            part_size = int(status.getLen())
            break
    if part_hadoop_path is None:
        raise RuntimeError(f"No parquet part file written under {staging_dir}")

    final_hadoop_path = jvm.org.apache.hadoop.fs.Path(final_uri)
    if not fs.rename(part_hadoop_path, final_hadoop_path):
        raise RuntimeError(f"Failed to move {part_hadoop_path} to {final_hadoop_path}")
    fs.delete(staging_path, True)

    equality_field_ids = spark.sparkContext._gateway.new_array(jvm.int, 1)
    equality_field_ids[0] = id_field_id

    delete_file = (
        ice.FileMetadata.deleteFileBuilder(table.spec())
        .ofEqualityDeletes(equality_field_ids)
        .withPath(_strip_file_uri_scheme(final_hadoop_path.toString()))
        .withFileSizeInBytes(part_size)
        .withRecordCount(len(ids))
        .withFormat(ice.FileFormat.PARQUET)
        .build()
    )
    table.newRowDelta().addDeletes(delete_file).commit()


@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors(started_cluster_iceberg_with_spark, storage_type, run_on_cluster):
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_deletion_vectors_" + storage_type + "_" + get_uuid_str()
    deleted_ids = [2, 5, 7, 100]

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(0, 200)")
    spark.sql(
        f"DELETE FROM {table_name} WHERE id IN ({', '.join(str(x) for x in deleted_ids)})"
    )

    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        run_on_cluster=run_on_cluster,
        table_function=True,
    )

    assert int(instance.query(f"SELECT count() FROM {expression}")) == 200 - len(deleted_ids)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == [
        x for x in range(200) if x not in deleted_ids
    ]


@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors_aggregates(started_cluster_iceberg_with_spark, storage_type, run_on_cluster):
    """Aggregates over Iceberg v3 tables must ignore rows covered by deletion vectors."""
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_deletion_vectors_aggregates_" + storage_type + "_" + get_uuid_str()
    deleted_ids = {2, 5, 7, 50, 99}
    remaining_ids = [i for i in range(100) if i not in deleted_ids]
    # value = 10 * id, so sum/avg expectations stay integer-friendly where possible.
    remaining_values = [10 * i for i in remaining_ids]

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint, value bigint, group_id int) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table_name}
        SELECT id, 10 * id, CAST(id % 3 AS INT)
        FROM range(0, 100)
        """
    )
    spark.sql(
        f"DELETE FROM {table_name} WHERE id IN ({', '.join(str(x) for x in sorted(deleted_ids))})"
    )

    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        run_on_cluster=run_on_cluster,
        table_function=True,
    )

    spark_row = spark.sql(
        f"""
        SELECT
            count(*) AS cnt,
            sum(id) AS sum_id,
            sum(value) AS sum_value,
            min(id) AS min_id,
            max(id) AS max_id,
            avg(value) AS avg_value
        FROM {table_name}
        """
    ).collect()[0]

    expected_count = len(remaining_ids)
    expected_sum_id = sum(remaining_ids)
    expected_sum_value = sum(remaining_values)
    expected_min_id = min(remaining_ids)
    expected_max_id = max(remaining_ids)
    expected_avg_value = expected_sum_value / expected_count

    assert spark_row["cnt"] == expected_count
    assert spark_row["sum_id"] == expected_sum_id
    assert spark_row["sum_value"] == expected_sum_value
    assert spark_row["min_id"] == expected_min_id
    assert spark_row["max_id"] == expected_max_id
    assert abs(float(spark_row["avg_value"]) - expected_avg_value) < 1e-9

    ch_row = instance.query(
        f"""
        SELECT
            count(),
            count(id),
            sum(id),
            sum(value),
            min(id),
            max(id),
            avg(value),
            uniqExact(id),
            countIf(id % 2 = 0),
            sumIf(value, id % 2 = 0)
        FROM {expression}
        """
    ).strip().split("\t")

    assert int(ch_row[0]) == expected_count
    assert int(ch_row[1]) == expected_count
    assert int(ch_row[2]) == expected_sum_id
    assert int(ch_row[3]) == expected_sum_value
    assert int(ch_row[4]) == expected_min_id
    assert int(ch_row[5]) == expected_max_id
    assert abs(float(ch_row[6]) - expected_avg_value) < 1e-9
    assert int(ch_row[7]) == expected_count

    expected_even_ids = [i for i in remaining_ids if i % 2 == 0]
    expected_count_if = len(expected_even_ids)
    expected_sum_if = sum(10 * i for i in expected_even_ids)
    assert int(ch_row[8]) == expected_count_if
    assert int(ch_row[9]) == expected_sum_if

    # Trivial COUNT must match the full scan once deletion vectors are applied.
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                settings={"optimize_trivial_count_query": 1},
            )
        )
        == expected_count
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                settings={"optimize_trivial_count_query": 0},
            )
        )
        == expected_count
    )

    # GROUP BY aggregates must also exclude deleted rows.
    spark_groups = {
        int(row["group_id"]): (int(row["cnt"]), int(row["sum_value"]))
        for row in spark.sql(
            f"""
            SELECT group_id, count(*) AS cnt, sum(value) AS sum_value
            FROM {table_name}
            GROUP BY group_id
            ORDER BY group_id
            """
        ).collect()
    }
    ch_groups = {}
    for line in instance.query(
        f"""
        SELECT group_id, count(), sum(value)
        FROM {expression}
        GROUP BY group_id
        ORDER BY group_id
        """
    ).strip().split("\n"):
        group_id, cnt, sum_value = line.split("\t")
        ch_groups[int(group_id)] = (int(cnt), int(sum_value))

    expected_groups = {}
    for group_id in (0, 1, 2):
        ids = [i for i in remaining_ids if i % 3 == group_id]
        expected_groups[group_id] = (len(ids), sum(10 * i for i in ids))

    assert spark_groups == expected_groups
    assert ch_groups == expected_groups


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors_complex(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_deletion_vectors_complex_" + storage_type + "_" + get_uuid_str()

    def expected_complex_ids():
        ids = list(range(20, 90)) + list(range(100, 150))
        ids += [x for x in range(200, 250) if x not in {205, 210, 220}]
        return sorted(ids)

    expected_ids = expected_complex_ids()

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint, data string) USING iceberg
        PARTITIONED BY (bucket(5, id))
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"INSERT INTO {table_name} SELECT id, char(id + ascii('a')) FROM range(10, 100)"
    )
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert int(instance.query(f"SELECT count(id) FROM {expression}")) == 90
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == list(range(10, 100))

    spark.sql(f"DELETE FROM {table_name} WHERE id < 20")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == list(range(20, 100))

    spark.sql(f"DELETE FROM {table_name} WHERE id >= 90")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == list(range(20, 90))

    spark.sql(
        f"INSERT INTO {table_name} SELECT id, char(id + ascii('a')) FROM range(100, 200)"
    )
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == list(range(20, 90)) + list(
        range(100, 200)
    )

    spark.sql(f"DELETE FROM {table_name} WHERE id >= 150")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == list(range(20, 90)) + list(
        range(100, 150)
    )

    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (label string)")
    spark.sql(
        f"""
        INSERT INTO {table_name}
        SELECT id, char(id + ascii('a')), 'new'
        FROM range(200, 250)
        """
    )
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == list(range(20, 90)) + list(
        range(100, 150)
    ) + list(range(200, 250))
    assert int(instance.query(f"SELECT count(id) FROM {expression} WHERE label = 'new'")) == 50

    spark.sql(f"DELETE FROM {table_name} WHERE id IN (205, 210, 220)")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == expected_ids
    assert int(instance.query(f"SELECT count(id) FROM {expression}")) == len(expected_ids)

    spark.sql(f"UPDATE {table_name} SET label = 'updated' WHERE id = 25")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert instance.query(f"SELECT label FROM {expression} WHERE id = 25").strip() == "updated"
    assert int(instance.query(f"SELECT count(id) FROM {expression} WHERE label = 'updated'")) == 1

    spark.sql(f"CALL system.rewrite_data_files(table => '{table_name}')")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == expected_ids
    # After rewrite, snapshot summary may still report stale total-position-deletes while data
    # files already have deletes applied. Trivial count must not subtract those and under-count.
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                settings={
                    "optimize_trivial_count_query": 1,
                    "use_iceberg_metadata_files_cache": 0,
                },
            )
        )
        == len(expected_ids)
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM (SELECT * FROM {expression})",
                settings={"use_iceberg_metadata_files_cache": 0},
            )
        )
        == len(expected_ids)
    )

    assert get_array(
        instance.query(
            f"SELECT id FROM {expression} WHERE id % 3 = 0"
        )
    ) == sorted([x for x in expected_ids if x % 3 == 0])


@pytest.mark.parametrize("storage_type", ["s3"])
def test_deletion_vectors_count_after_rewrite_data_files(
    started_cluster_iceberg_with_spark, storage_type
):
    """Regression: Spark rewrite can leave total-position-deletes in the snapshot summary after
    deletes were already applied into rewritten data files. SELECT count() must still match the scan.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_dv_count_after_rewrite_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            id bigint,
            data string
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table_name}
        SELECT id, concat('row-', CAST(id AS STRING)) FROM range(100)
        """
    )
    spark.sql(f"DELETE FROM {table_name} WHERE id % 10 = 0")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )
    expected = [x for x in range(100) if x % 10 != 0]
    settings = {"use_iceberg_metadata_files_cache": 0}

    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == expected
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                settings={**settings, "optimize_trivial_count_query": 1},
            )
        )
        == 90
    )

    spark.sql(
        f"""
        CALL system.rewrite_data_files(
            table => '{table_name}',
            options => map('delete-file-threshold', '1')
        )
        """
    )
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == expected
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                settings={**settings, "optimize_trivial_count_query": 1},
            )
        )
        == 90
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM (SELECT * FROM {expression})",
                settings=settings,
            )
        )
        == 90
    )


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors_trivial_count_fails_closed_with_live_deletes(
    started_cluster_iceberg_with_spark, storage_type
):
    """Live DVs / equality deletes must not use snapshot-summary trivial COUNT.

    Summary fields are optional and can disagree with manifests. With any live
    position deletes (puffin DVs) the summary shortcut must stay closed:
    IcebergTrivialCountOptimizationApplied == 0, and count() must match a scan.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_dv_trivial_count_closed_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            id bigint,
            data string
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table_name}
        SELECT id, concat('row-', CAST(id AS STRING)) FROM range(100)
        """
    )
    spark.sql(f"DELETE FROM {table_name} WHERE id % 10 = 0")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )
    settings = {
        "optimize_trivial_count_query": 1,
        "use_iceberg_metadata_files_cache": 0,
    }
    expected_after_dv = [x for x in range(100) if x % 10 != 0]

    instance.query("SYSTEM DROP ICEBERG METADATA CACHE")

    query_id_dv = f"{table_name}-dv-{uuid.uuid4()}"
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                query_id=query_id_dv,
                settings=settings,
            )
        )
        == len(expected_after_dv)
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM (SELECT * FROM {expression})",
                settings={"use_iceberg_metadata_files_cache": 0},
            )
        )
        == len(expected_after_dv)
    )
    assert get_array(
        instance.query(
            f"SELECT id FROM {expression}",
            settings={"use_iceberg_metadata_files_cache": 0},
        )
    ) == expected_after_dv

    instance.query("SYSTEM FLUSH LOGS")
    assert (
        int(
            instance.query(
                f"""
                SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied']
                FROM system.query_log
                WHERE query_id = '{query_id_dv}' AND type = 'QueryFinish'
                """
            )
        )
        == 0
    )

    # Equality deletes whose optional summary field may stay 0 must still fail closed
    # while the DV remains live (and after, while equality-delete files remain).
    add_equality_deletes_by_id(spark, table_name, [1, 5])
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)
    expected_after_eq = [x for x in expected_after_dv if x not in (1, 5)]

    instance.query("SYSTEM DROP ICEBERG METADATA CACHE")
    query_id_eq = f"{table_name}-eq-{uuid.uuid4()}"
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                query_id=query_id_eq,
                settings=settings,
            )
        )
        == len(expected_after_eq)
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM (SELECT * FROM {expression})",
                settings={"use_iceberg_metadata_files_cache": 0},
            )
        )
        == len(expected_after_eq)
    )

    instance.query("SYSTEM FLUSH LOGS")
    assert (
        int(
            instance.query(
                f"""
                SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied']
                FROM system.query_log
                WHERE query_id = '{query_id_eq}' AND type = 'QueryFinish'
                """
            )
        )
        == 0
    )


def _poison_snapshot_total_records(table_name, poisoned_total="999999"):
    """Overwrite total-records in every snapshot summary of the latest metadata JSON.

    Simulates a corrupted / incorrectly-maintained incremental summary while leaving
    data files and manifests consistent.
    """
    import json
    import glob as glob_mod

    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    metadata_files = sorted(glob_mod.glob(f"{metadata_dir}/v*.metadata.json"))
    if not metadata_files:
        raise RuntimeError(f"No metadata JSON under {metadata_dir}")
    path = metadata_files[-1]
    with open(path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    for snapshot in meta.get("snapshots", []):
        summary = snapshot.setdefault("summary", {})
        summary["total-records"] = poisoned_total
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_trivial_count_prefers_manifests_over_poisoned_summary(
    started_cluster_iceberg_with_spark, storage_type
):
    """Poisoned snapshot-summary total-records must not become SELECT count().

    Manifest per-file record_count is ground truth; summary is warning-only.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_poisoned_summary_count_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            id bigint,
            data string
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '3')
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table_name}
        SELECT id, concat('row-', CAST(id AS STRING)) FROM range(50)
        """
    )
    _poison_snapshot_total_records(table_name, "999999")
    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )
    settings = {
        "optimize_trivial_count_query": 1,
        "use_iceberg_metadata_files_cache": 0,
    }

    instance.query("SYSTEM DROP ICEBERG METADATA CACHE")
    query_id = f"{table_name}-{uuid.uuid4()}"
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                query_id=query_id,
                settings=settings,
            )
        )
        == 50
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM (SELECT * FROM {expression})",
                settings={"use_iceberg_metadata_files_cache": 0},
            )
        )
        == 50
    )

    instance.query("SYSTEM FLUSH LOGS")
    # Manifest-sum path still counts as the trivial COUNT optimization.
    assert (
        int(
            instance.query(
                f"""
                SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied']
                FROM system.query_log
                WHERE query_id = '{query_id}' AND type = 'QueryFinish'
                """
            )
        )
        == 1
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_deletion_vectors_puffin_files_cache(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_deletion_vectors_cache_" + storage_type + "_" + get_uuid_str()
    deleted_ids = [2, 5, 7, 100]

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(0, 200)")
    spark.sql(
        f"DELETE FROM {table_name} WHERE id IN ({', '.join(str(x) for x in deleted_ids)})"
    )

    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    instance.query("SYSTEM DROP PUFFIN FILES CACHE")

    query_id1 = f"{table_name}-{uuid.uuid4()}"
    query_id2 = f"{table_name}-{uuid.uuid4()}"
    query_id3 = f"{table_name}-{uuid.uuid4()}"

    assert int(
        instance.query(
            f"SELECT count(id) FROM {expression}",
            query_id=query_id1,
            settings={"use_puffin_files_cache": 1},
        )
    ) == 200 - len(deleted_ids)

    assert int(
        instance.query(
            f"SELECT count(id) FROM {expression}",
            query_id=query_id2,
            settings={"use_puffin_files_cache": 1},
        )
    ) == 200 - len(deleted_ids)

    instance.query("SYSTEM FLUSH LOGS")

    assert int(
        instance.query(
            f"SELECT ProfileEvents['PuffinFilesCacheMisses'] FROM system.query_log WHERE query_id = '{query_id1}' AND type = 'QueryFinish'"
        )
    ) > 0
    assert int(
        instance.query(
            f"SELECT ProfileEvents['PuffinFilesCacheHits'] FROM system.query_log WHERE query_id = '{query_id2}' AND type = 'QueryFinish'"
        )
    ) > 0

    puffin_reads_first = int(
        instance.query(
            f"SELECT ProfileEvents['PuffinFilesRead'] FROM system.query_log WHERE query_id = '{query_id1}' AND type = 'QueryFinish'"
        )
    )
    puffin_reads_second = int(
        instance.query(
            f"SELECT ProfileEvents['PuffinFilesRead'] FROM system.query_log WHERE query_id = '{query_id2}' AND type = 'QueryFinish'"
        )
    )
    assert puffin_reads_first > 0
    assert puffin_reads_second == 0

    instance.query("SYSTEM DROP PUFFIN FILES CACHE")

    assert int(
        instance.query(
            f"SELECT count(id) FROM {expression}",
            query_id=query_id3,
            settings={"use_puffin_files_cache": 1},
        )
    ) == 200 - len(deleted_ids)

    instance.query("SYSTEM FLUSH LOGS")

    assert int(
        instance.query(
            f"SELECT ProfileEvents['PuffinFilesCacheMisses'] FROM system.query_log WHERE query_id = '{query_id3}' AND type = 'QueryFinish'"
        )
    ) > int(
        instance.query(
            f"SELECT ProfileEvents['PuffinFilesCacheMisses'] FROM system.query_log WHERE query_id = '{query_id2}' AND type = 'QueryFinish'"
        )
    )


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors_reject_mutations(started_cluster_iceberg_with_spark, storage_type):
    """DELETE/UPDATE must fail closed on tables that already contain deletion vectors.

    ClickHouse mutations write parquet position-delete files, which Iceberg readers ignore for
    data files that have a matching DV — so a successful mutation would silently leave rows.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_deletion_vectors_reject_mutations_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(0, 20)")
    spark.sql(f"DELETE FROM {table_name} WHERE id IN (1, 2, 3)")

    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    instance.query(
        get_creation_expression(
            storage_type,
            table_name,
            started_cluster_iceberg_with_spark,
            table_function=False,
        )
    )

    assert int(instance.query(f"SELECT count() FROM {table_name}")) == 17

    delete_error = instance.query_and_get_error(
        f"ALTER TABLE {table_name} DELETE WHERE id = 4",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "deletion vectors" in delete_error.lower()

    update_error = instance.query_and_get_error(
        f"ALTER TABLE {table_name} UPDATE id = 0 WHERE id = 4",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "deletion vectors" in update_error.lower()

    # Rows must be unchanged after rejected mutations.
    assert int(instance.query(f"SELECT count() FROM {table_name}")) == 17
    assert get_array(instance.query(f"SELECT id FROM {table_name}")) == [
        x for x in range(20) if x not in (1, 2, 3)
    ]


@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors_with_equality_deletes(
    started_cluster_iceberg_with_spark, storage_type, run_on_cluster
):
    """DV + equality deletes on the same table must keep correct survivors.

    Guards StorageObjectStorageSource transform order: DV must run before equality
    FilterTransform (see gtest_deletion_vector_before_equality_filter).
    """
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_dv_with_eq_" + storage_type + "_" + get_uuid_str()

    # Small unpartitioned file so DV and equality deletes both apply to the same data file.
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(0, 20)")
    # Deletion vector removes file positions for these ids (values equal positions here).
    spark.sql(f"DELETE FROM {table_name} WHERE id IN (2, 7)")
    # Equality deletes remove by value after DV materialization.
    add_equality_deletes_by_id(spark, table_name, [1, 5])

    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        run_on_cluster=run_on_cluster,
        table_function=True,
    )

    deleted = {1, 2, 5, 7}
    expected = [x for x in range(20) if x not in deleted]
    spark_ids = sorted(int(r[0]) for r in spark.sql(f"SELECT id FROM {table_name}").collect())
    assert spark_ids == expected

    assert int(instance.query(f"SELECT count() FROM {expression}")) == len(expected)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == expected


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_deletion_vectors_cluster_bucket_split(started_cluster_iceberg_with_spark, storage_type):
    """icebergCluster bucket splitting must preserve DV (and clone) metadata end-to-end."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_dv_cluster_bucket_" + storage_type + "_" + get_uuid_str()
    deleted_ids = [2, 5, 7, 100]

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read',
            'write.parquet.row-group-size-bytes' = '1'
        )
        """
    )
    spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(0, 200)")
    spark.sql(
        f"DELETE FROM {table_name} WHERE id IN ({', '.join(str(x) for x in deleted_ids)})"
    )

    upload_table(started_cluster_iceberg_with_spark, storage_type, table_name)

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        run_on_cluster=True,
        table_function=True,
    )
    expected = [x for x in range(200) if x not in deleted_ids]
    settings = {
        "cluster_table_function_split_granularity": "bucket",
        "cluster_table_function_buckets_batch_size": 1,
    }

    assert (
        int(instance.query(f"SELECT count() FROM {expression}", settings=settings))
        == len(expected)
    )
    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == expected
    # Trivial count path must match under bucket splits (need_only_count + DV cardinality).
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression}",
                settings={**settings, "optimize_trivial_count_query": 1},
            )
        )
        == len(expected)
    )

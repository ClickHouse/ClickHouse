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

    assert get_array(
        instance.query(
            f"SELECT id FROM {expression} WHERE id % 3 = 0"
        )
    ) == sorted([x for x in expected_ids if x % 3 == 0])


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

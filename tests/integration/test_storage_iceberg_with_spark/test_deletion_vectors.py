import uuid

import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
)


def get_array(query_result: str):
    return sorted([int(x) for x in query_result.strip().split("\n") if x])


def expected_complex_ids():
    ids = list(range(20, 90)) + list(range(100, 150))
    ids += [x for x in range(200, 250) if x not in {205, 210, 220}]
    return sorted(ids)


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


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors_complex(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_deletion_vectors_complex_" + storage_type + "_" + get_uuid_str()
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

    instance.query("SYSTEM DROP PUFFIN_FILES_CACHE")

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

    instance.query("SYSTEM DROP PUFFIN_FILES_CACHE")

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

import pytest
import json

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    create_iceberg_table,
    get_creation_expression
)


def get_array(query_result: str):
    arr = sorted([int(x) for x in query_result.strip().split("\n")])
    print(arr)
    return arr

def patch_metadata(table_name):
    # HACK This is terribly ugly hack, because of the issue:https://github.com/apache/iceberg/issues/13634
    # Iceberg sort order looks relatively new feature. There are no writer implementations which support it properly.
    # For example pyiceberg doesn't support it at all, you can specify sort order, but data will be written unsorted.
    # Spark implementation supports it, i.e. writes sorted data, but doesn't write proper sort_order_id in manifest files (always writes 0).
    # Here we manually modify metadata file to set actual sort order to id 0.
    with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata/v4.metadata.json", "rb") as f:
        content = json.load(f)
        for order in content['sort-orders']:
            if order['order-id'] == 1:
                order_found = order
                break
        else:
            raise Exception("Sort order with id 1 not found")
        order_found['order-id'] = 0
        content['sort-orders'] = [order_found]
        content['default-sort-order-id'] = 0

        with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata/v4.metadata.json", "w") as out_f:
            json.dump(content, out_f)
    # HACK END


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_read_in_order(started_cluster_iceberg_with_spark,  storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
    """)
    spark.sql(f"""
        ALTER TABLE {TABLE_NAME} 
        WRITE ORDERED BY id
    """)

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'a'), (3, 'c')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2,'d'), (4, 'f')")

    patch_metadata(TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query_id = get_uuid_str()

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergBucket(16, id);"
        )
    )

    assert 'MergingSortedTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert get_array(instance.query(f"SELECT distinct(id) FROM {TABLE_NAME}", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT distinct(id) FROM {TABLE_NAME};"
        )
    )

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY (id, data)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY (id, data);"
        )
    )

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME}", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME};"
        )
    )

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY (data, id)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY (data, id);"
        )
    )

def test_defining_columns_with_special_character(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "demo_event_" + get_uuid_str()
    spark = started_cluster_iceberg_with_spark.spark_session

    spark.sql(
        f"""
            CREATE TABLE {table_name}
            (
            `#event` STRING NOT NULL ,
            `#data_lifecycle` STRING NOT NULL,
            `#time` TIMESTAMP NOT NULL ,
            `#log_id` STRING NOT NULL ,
            `#ingest_time` TIMESTAMP )
            USING iceberg
            PARTITIONED BY (`#event`, `#time`)
            TBLPROPERTIES (
            'identifier-fields' = '[#data_lifecycle,#event,#log_id]',
            'sort-order' = '#data_lifecycle ASC NULLS FIRST, #event ASC NULLS FIRST, #time ASC NULLS FIRST'
            )
        """
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "s3",
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )

    table_expr = get_creation_expression("s3", table_name, started_cluster_iceberg_with_spark, table_function=True)

    instance.query(f"SELECT * FROM {table_expr}")

    spark.sql(
        f"""
            INSERT INTO {table_name} VALUES
            ('click', 'active', TIMESTAMP '2024-01-01 00:00:00', 'log1', TIMESTAMP '2024-01-01 00:00:01'),
            ('view', 'active', TIMESTAMP '2024-01-02 00:00:00', 'log2', NULL)
        """
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "s3",
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )
    instance.query(f"SELECT * FROM {table_expr}")
    spark.sql(f"DROP TABLE {table_name}")


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_read_in_order_with_complex_bucket(started_cluster_iceberg_with_spark,  storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
    """)
    spark.sql(f"""
        ALTER TABLE {TABLE_NAME} 
        WRITE ORDERED BY bucket(16, id)
    """)

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'a'), (3, 'c')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2,'d'), (4, 'f')")

    patch_metadata(TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query_id = get_uuid_str()

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY icebergBucket(16, id)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergBucket(16, id);"
        )
    )

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_read_in_order_with_complex_truncate(started_cluster_iceberg_with_spark,  storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
    """)
    spark.sql(f"""
        ALTER TABLE {TABLE_NAME} 
        WRITE ORDERED BY truncate(16, id)
    """)

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'a'), (3, 'c')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2,'d'), (4, 'f')")

    patch_metadata(TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query_id = get_uuid_str()

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY icebergTruncate(16, id)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergBucket(16, id);"
        )
    )

    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergTruncate(16, id);"
        )
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_read_in_order_through_merge_table(started_cluster_iceberg_with_spark, storage_type):
    # An object storage table reached through a `Merge` table must not be told to
    # read in an order it cannot deliver: `ReadFromObjectStorageStep` has no
    # reverse file walk and no `ReverseTransform`, so it only ever promises the
    # natural order. The direct path is where that gate is observable today, and
    # this test pins it, together with the current `Merge` behaviour.
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_merge_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
    """)
    spark.sql(f"""
        ALTER TABLE {TABLE_NAME}
        WRITE ORDERED BY id
    """)

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'a'), (3, 'c')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2,'d'), (4, 'f')")

    patch_metadata(TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    merge_source = f"merge(currentDatabase(), '^{TABLE_NAME}$')"

    # Reading in order through a `Merge` table over an object storage table is
    # rejected in either direction, for two independent reasons. First, a
    # persistent `Iceberg` table only learns its sort order when
    # `updateExternalDynamicMetadataIfExists` is called, which the analyzer does
    # for the tables named in the query - and a `Merge` table does not forward it
    # to the tables it selects - so while the `Merge` table is the only reader the
    # child's sorting key is empty and `checkSupportedReadingStep` rejects the
    # optimization outright. Second, even with refreshed metadata, the object
    # storage arm of `recursivelyApplyToReadingSteps` fails closed:
    # `ReadFromObjectStorageStep::initializePipeline` does not preserve file order
    # (https://github.com/ClickHouse/ClickHouse/issues/112981), so the outer step
    # must not announce an order the child reader does not deliver. The `Merge`
    # queries here come first, before any direct read has refreshed the child's
    # metadata, so this block exercises the first gate; the block at the end of
    # the test exercises the second.
    assert instance.query(
        f"SELECT id FROM {merge_source} ORDER BY id"
    ).strip().split("\n") == ["1", "2", "3", "4"]
    assert "PartialSortingTransform" in (
        instance.query(f"EXPLAIN PIPELINE SELECT id FROM {merge_source} ORDER BY id")
    )

    assert instance.query(
        f"SELECT id FROM {merge_source} ORDER BY id DESC"
    ).strip().split("\n") == ["4", "3", "2", "1"]
    assert "PartialSortingTransform" in (
        instance.query(f"EXPLAIN PIPELINE SELECT id FROM {merge_source} ORDER BY id DESC")
    )

    # The direct path is where reading in order really engages. Ascending is
    # accepted, so the sorting step is replaced by a merge of the already sorted
    # streams. That the request is accepted is what this assertion pins - it is
    # the positive control for the direction gate asserted right below.
    #
    # The delivered row order is deliberately not asserted here: every source in
    # `ReadFromObjectStorageStep::initializePipeline` pulls from one shared file
    # iterator, so which data file a given stream reads is a race. When a single
    # stream happens to take both files its output is their concatenation, which
    # is not sorted (the files overlap: `1, 3` and `2, 4`), and the merge above it
    # has nothing left to interleave. See
    # https://github.com/ClickHouse/ClickHouse/issues/112981 - a pre-existing
    # limitation of reading an object storage table in order, unrelated to this
    # direction gate. The sibling `test_read_in_order` in this file sorts its
    # results through `get_array` for the same reason.
    assert sorted(
        int(x) for x in instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id").strip().split("\n")
    ) == [1, 2, 3, 4]
    assert "PartialSortingTransform" not in (
        instance.query(f"EXPLAIN PIPELINE SELECT id FROM {TABLE_NAME} ORDER BY id")
    )

    # The reverse direction is not supported by the reader, so it must be
    # rejected and the sorting step kept - otherwise ascending chunks would be
    # announced as descending and the rows would come out in the wrong order.
    # This is the regression test for the direction gate in
    # `ReadFromObjectStorageStep::requestReadingInOrder`.
    assert instance.query(
        f"SELECT id FROM {TABLE_NAME} ORDER BY id DESC"
    ).strip().split("\n") == ["4", "3", "2", "1"]
    assert "PartialSortingTransform" in (
        instance.query(f"EXPLAIN PIPELINE SELECT id FROM {TABLE_NAME} ORDER BY id DESC")
    )

    # The direct reads above refreshed the child's metadata (cached in the
    # storage object), so from here on the child's sorting key is visible through
    # the `Merge` table and `checkSupportedReadingStep` no longer stands in the
    # way. The fail-closed object storage arm of `recursivelyApplyToReadingSteps`
    # must still reject the request and keep the sorting step: the object storage
    # pipeline does not preserve file order, so accepting here would return rows
    # in a racy order (https://github.com/ClickHouse/ClickHouse/issues/112981).
    # Once that issue is fixed and the `Merge` path delegates to
    # `ReadFromObjectStorageStep::requestReadingInOrder`, flip the ascending
    # assertion to expect the sorting step to be dropped.
    assert instance.query(
        f"SELECT id FROM {merge_source} ORDER BY id"
    ).strip().split("\n") == ["1", "2", "3", "4"]
    assert "PartialSortingTransform" in (
        instance.query(f"EXPLAIN PIPELINE SELECT id FROM {merge_source} ORDER BY id")
    )

    # A `Merge` table that mixes a `MergeTree` child with the object storage child: the
    # in-order request must be all-or-nothing. The parent rejects it, because the object
    # storage child cannot preserve order, so no child may be left in read-in-order mode
    # either - otherwise the `MergeTree` child would pay the whole cost of reading in order
    # (a narrowed stream budget, `has_outer_limit`, the per-part `PrefetchingConcat`
    # safeguards) while the parent sorts the result anyway.
    #
    # `EXPLAIN PIPELINE` does not descend into the child pipeline of a `Merge` table, so the
    # reading mode of the child is observed through `system.processors_profile_log`: a
    # `MergeTree` reader switched into read-in-order mode is named
    # `MergeTreeSelect(pool: ..., algorithm: InOrder)`.
    MT_TABLE_NAME = TABLE_NAME + "_mt"
    instance.query(f"DROP TABLE IF EXISTS {MT_TABLE_NAME}")
    instance.query(
        f"CREATE TABLE {MT_TABLE_NAME} (id Int64, data String) ENGINE = MergeTree ORDER BY id"
    )
    instance.query(f"INSERT INTO {MT_TABLE_NAME} VALUES (5, 'g'), (7, 'i')")
    instance.query(f"INSERT INTO {MT_TABLE_NAME} VALUES (6, 'h'), (8, 'j')")

    def count_in_order_readers(query_id):
        instance.query("SYSTEM FLUSH LOGS processors_profile_log")
        return int(
            instance.query(
                "SELECT countIf(name LIKE '%algorithm: InOrder%') FROM system.processors_profile_log "
                f"WHERE query_id = '{query_id}'"
            ).strip()
        )

    # Positive control: over the `MergeTree` table alone the `Merge` table does read in order,
    # so the assertion for the mixed set below cannot pass vacuously.
    mt_merge_source = f"merge(currentDatabase(), '^{MT_TABLE_NAME}$')"
    mt_query_id = TABLE_NAME + "_merge_mt_only"
    assert instance.query(
        f"SELECT id FROM {mt_merge_source} ORDER BY id",
        query_id=mt_query_id,
        settings={"log_processors_profiles": 1},
    ).strip().split("\n") == ["5", "6", "7", "8"]
    assert count_in_order_readers(mt_query_id) > 0

    # The mixed child set is rejected as a whole: the result is still correct and sorted by
    # the parent, and no child reader was switched into read-in-order mode.
    mixed_merge_source = f"merge(currentDatabase(), '^{TABLE_NAME}(_mt)?$')"
    mixed_query_id = TABLE_NAME + "_merge_mixed"
    assert instance.query(
        f"SELECT id FROM {mixed_merge_source} ORDER BY id",
        query_id=mixed_query_id,
        settings={"log_processors_profiles": 1},
    ).strip().split("\n") == ["1", "2", "3", "4", "5", "6", "7", "8"]
    assert count_in_order_readers(mixed_query_id) == 0

    instance.query(f"DROP TABLE {MT_TABLE_NAME}")

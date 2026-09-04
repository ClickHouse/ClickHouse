import pytest
import glob
import json
import os
import re

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

def get_array_as_returned(query_result: str):
    """Rows in the order the server returned them. Unlike get_array, does not sort."""
    arr = [int(x) for x in query_result.strip().split("\n")]
    print(arr)
    return arr

def top_step_output_columns(instance, query, step, settings=""):
    """Columns the topmost processor of `step` emits, from EXPLAIN PIPELINE header=1.

    A header block spans one line per column, so reading only the first line would miss exactly
    the leaked-column case this exists to catch. Every column line carries "(size = "; the next
    processor line does not, which is what terminates the block."""
    suffix = f" SETTINGS {settings}" if settings else ""
    plan = instance.query(f"EXPLAIN PIPELINE header=1 {query}{suffix}")
    body = plan.split(f"({step})")[1].strip().split("\n")
    header_lines = []
    for line in body[1:]:
        if "(size = " not in line:
            break
        header_lines.append(line.strip())
    assert header_lines, body[:4]
    header_lines[0] = header_lines[0].removeprefix("Header: ")
    return [line.split(":")[0].strip() for line in header_lines]

def count_in_pipeline(instance, query, processor, settings=""):
    """Occurrences of `processor` in EXPLAIN PIPELINE. Result assertions alone cannot tell the
    in-order topology from a correct fallback, so the plan has to be asserted directly."""
    suffix = f" SETTINGS {settings}" if settings else ""
    return int(
        instance.query(
            f"SELECT count() FROM (EXPLAIN PIPELINE {query}{suffix}) "
            f"WHERE explain ILIKE '%{processor}%'"
        )
    )

def patch_metadata(table_name):
    # HACK This is terribly ugly hack, because of the issue:https://github.com/apache/iceberg/issues/13634
    # Iceberg sort order looks relatively new feature. There are no writer implementations which support it properly.
    # For example pyiceberg doesn't support it at all, you can specify sort order, but data will be written unsorted.
    # Spark implementation supports it, i.e. writes sorted data, but doesn't write proper sort_order_id in manifest files (always writes 0).
    # Here we manually modify metadata file to set actual sort order to id 0.
    # Each INSERT adds a metadata version, so patch the newest one rather than a fixed name -
    # patching a stale version leaves the table unsorted and silently measures the declined path.
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    versions = [
        (int(re.match(r"v(\d+)\.metadata\.json", os.path.basename(path)).group(1)), path)
        for path in glob.glob(f"{metadata_dir}/v*.metadata.json")
    ]
    if not versions:
        raise Exception(f"No metadata files found in {metadata_dir}")
    _, metadata_path = max(versions)

    with open(metadata_path, "rb") as f:
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

        with open(metadata_path, "w") as out_f:
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

    # The sorting step is dropped for this query, so the rows must already arrive sorted.
    # max_threads=1 is the deterministic case: one stream must not concatenate both files.
    for max_threads in [1, 2, 4]:
        assert get_array_as_returned(
            instance.query(
                f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS max_threads={max_threads}",
                query_id=query_id,
            )
        ) == [1,2,3,4]
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

    # DESC is declined (no reverse file walk), so the sorting step must be kept and every row
    # must still be returned. A decline that loses rows would be worse than the unsorted read.
    assert get_array_as_returned(
        instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id DESC SETTINGS max_threads=1")
    ) == [4,3,2,1]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id DESC;"
        )
    )

    # Ordering must not depend on the positional-delete setting that happens to pin
    # preserve_order today.
    assert get_array_as_returned(
        instance.query(
            f"SELECT id FROM {TABLE_NAME} ORDER BY id "
            f"SETTINGS max_threads=1, use_roaring_bitmap_iceberg_positional_deletes=1"
        )
    ) == [1,2,3,4]

    # Every declined path must return the full row set.
    for settings in [
        "max_threads=1, read_in_order_two_level_merge_threshold=0",
        "max_threads=1, optimize_read_in_order=0",
        "max_threads=1",
    ]:
        assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} SETTINGS {settings}")) == [1,2,3,4]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_read_in_order_more_files_than_streams(started_cluster_iceberg_with_spark, storage_type):
    """Exercise the preliminary-merge grouping: more files than output streams."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_many_" + storage_type + "_" + get_uuid_str()

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

    # Six overlapping files: each holds a low and a high id, so a concatenation is never sorted.
    for i in range(1, 7):
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({i},'d{i}'), ({i + 6},'d{i + 6}')")

    patch_metadata(TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    expected = list(range(1, 13))

    # threshold=2 with 4 streams groups the 6 files into 3 merged groups; threshold=100 keeps one
    # port per file. Both must be order-preserving and lose no rows.
    # threshold=0 means "always preliminary-merge" and must not divide by zero: with 6 files it
    # only reaches the grouping once the cap (threshold * num_streams) admits them, i.e. at
    # max_threads >= 6. (4, 0) declines and (6, 0) groups; keep both.
    for max_threads, threshold in [(4, 2), (4, 3), (6, 0), (4, 0), (4, 1), (2, 100), (4, 100), (1, 100)]:
        settings = f"max_threads={max_threads}, read_in_order_two_level_merge_threshold={threshold}"
        assert get_array_as_returned(
            instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS {settings}")
        ) == expected
        assert instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS {settings}").strip() == "12"
        # The temporary sorting-key columns the grouping merge adds must be projected away.
        assert instance.query(
            f"SELECT * FROM {TABLE_NAME} ORDER BY id LIMIT 1 SETTINGS {settings}"
        ).strip() == "1\td1"

    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT id FROM {TABLE_NAME} ORDER BY id "
            f"SETTINGS max_threads=4, read_in_order_two_level_merge_threshold=2;"
        )
    )

    # The grouping topology, not just its result: one sorted run per port plus the downstream
    # merge already sorts correctly, so only the plan shape distinguishes the two.
    q = f"SELECT id FROM {TABLE_NAME} ORDER BY id"
    n_grouped = count_in_pipeline(
        instance, q, "MergingSortedTransform",
        "max_threads=4, read_in_order_two_level_merge_threshold=2")
    n_flat = count_in_pipeline(
        instance, q, "MergingSortedTransform",
        "max_threads=4, read_in_order_two_level_merge_threshold=100")
    assert n_flat == 1, n_flat
    assert n_grouped == 4, n_grouped
    # 6 files / threshold 3 -> 2 groups, so one fewer preliminary merge than threshold 2.
    assert count_in_pipeline(
        instance, q, "MergingSortedTransform",
        "max_threads=4, read_in_order_two_level_merge_threshold=3") == 3


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_read_in_order_complex_key_more_files_than_streams(started_cluster_iceberg_with_spark, storage_type):
    """An expression sorting key is absent from the read step's header, so the grouping merge has
    to materialize it and then project it away."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_many_bucket_" + storage_type + "_" + get_uuid_str()

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

    for i in range(1, 7):
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({i},'d{i}'), ({i + 6},'d{i + 6}')")

    patch_metadata(TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    q = f"SELECT icebergBucket(16, id) FROM {TABLE_NAME} ORDER BY icebergBucket(16, id)"

    for max_threads, threshold in [(4, 2), (4, 100)]:
        settings = f"max_threads={max_threads}, read_in_order_two_level_merge_threshold={threshold}"
        buckets = get_array_as_returned(instance.query(f"{q} SETTINGS {settings}"))
        assert buckets == sorted(buckets)
        assert len(buckets) == 12
        assert get_array(
            instance.query(f"SELECT id FROM {TABLE_NAME} SETTINGS {settings}")
        ) == list(range(1, 13))
        # The header must be unchanged by the materialize-merge-project bracket.
        assert len(instance.query(
            f"SELECT * FROM {TABLE_NAME} ORDER BY icebergBucket(16, id) LIMIT 1 SETTINGS {settings}"
        ).strip().split("\t")) == 2
        # An expression key IS accepted, so the sorting step is dropped for it. Without this the
        # sorted-bucket assertion above is also satisfied by an ordinary sort.
        assert count_in_pipeline(instance, q, "PartialSortingTransform", settings) == 0

    # Regression: the grouping merge materializes the sorting key into the pipe, and an identity
    # projection cannot remove it again (unmatched header columns are re-emitted), so the step
    # leaked a column absent from its declared output header. It must emit exactly the columns it
    # declares - the grouped and ungrouped shapes agree on that.
    for threshold in [2, 100]:
        assert top_step_output_columns(
            instance, q, "ReadFromObjectStorage",
            f"max_threads=4, read_in_order_two_level_merge_threshold={threshold}"
        ) == ["id Nullable(Int64)"], threshold


def test_read_in_order_orc_declines(started_cluster_iceberg_with_spark):
    """ORC has no preserve_order equivalent, so a single file is not a sorted run and the
    optimization must be declined - while still returning every row."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_orc_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
        TBLPROPERTIES ('write.format.default'='orc')
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
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS max_threads=1;"
        )
    )
    assert get_array_as_returned(
        instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS max_threads=1")
    ) == [1,2,3,4]
    assert instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS max_threads=1").strip() == "4"


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_read_in_order_duplicate_keys(started_cluster_iceberg_with_spark, storage_type):
    """Duplicate keys across files are what distinct-in-order and aggregation-in-order get wrong
    when an input port is not a sorted run: equal values must be contiguous."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_dup_" + storage_type + "_" + get_uuid_str()

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
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'x'), (2, 'b')")

    patch_metadata(TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    for max_threads in [1, 4]:
        settings = f"max_threads={max_threads}"
        assert get_array_as_returned(
            instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS {settings}")
        ) == [1,1,2,3]
        # DISTINCT without ORDER BY promises no ordering, so assert what distinct-in-order can
        # actually get wrong: a duplicate surviving because equal values were not contiguous.
        assert get_array(
            instance.query(f"SELECT DISTINCT id FROM {TABLE_NAME} SETTINGS {settings}")
        ) == [1,2,3]
        assert instance.query(
            f"SELECT id, count() FROM {TABLE_NAME} GROUP BY id ORDER BY id SETTINGS {settings}"
        ).strip() == "1\t2\n2\t1\n3\t1"
        assert get_array_as_returned(
            instance.query(f"SELECT DISTINCT id FROM {TABLE_NAME} ORDER BY id SETTINGS {settings}")
        ) == [1,2,3]

    # Aggregation-in-order consumes the same per-port sorted runs, so assert it is really the
    # operator producing the results above and not an ordinary hash aggregation.
    agg = f"SELECT id, count() FROM {TABLE_NAME} GROUP BY id"
    assert count_in_pipeline(
        instance, agg, "AggregatingInOrderTransform",
        "max_threads=4, optimize_aggregation_in_order=1") > 0
    assert count_in_pipeline(
        instance, agg, "AggregatingInOrderTransform",
        "max_threads=4, optimize_aggregation_in_order=0") == 0
    assert instance.query(
        f"{agg} ORDER BY id SETTINGS max_threads=4, optimize_aggregation_in_order=1"
    ).strip() == "1\t2\n2\t1\n3\t1"

    # Distinct-in-order cannot engage on this step: it only fires when it can IMPROVE on the
    # order the step already advertises, and getDataOrder advertises the full sorting key. So the
    # plain DistinctTransform above is the expected operator, not a silent fallback.
    assert count_in_pipeline(
        instance, f"SELECT DISTINCT id FROM {TABLE_NAME}", "DistinctSortedStreamTransform",
        "max_threads=4, optimize_distinct_in_order=1") == 0


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

import pytest

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

    # More files than threshold * max_threads: the read is declined and the sort is kept.
    for max_threads, threshold, declined in [(4, 0, True), (4, 1, True), (6, 0, False)]:
        settings = f"max_threads={max_threads}, read_in_order_two_level_merge_threshold={threshold}"
        assert (count_in_pipeline(instance, q, "PartialSortingTransform", settings) > 0) == declined, settings

    # A GLOBAL IN set is built only when the pipeline runs, so deciding the in-order read, which
    # evaluates the filter during planning, must decline and keep the sort instead.
    q_global_in = f"SELECT id FROM {TABLE_NAME} WHERE _path GLOBAL IN (SELECT _path FROM {TABLE_NAME}) ORDER BY id"
    assert get_array_as_returned(instance.query(f"{q_global_in} SETTINGS max_threads=4")) == expected
    assert count_in_pipeline(instance, q_global_in, "PartialSortingTransform", "max_threads=4") > 0
    # Planning alone must not run the subquery.
    instance.query(
        f"EXPLAIN SELECT id FROM {TABLE_NAME} "
        f"WHERE _path GLOBAL IN (SELECT toString(throwIf(1, 'subquery ran during planning'))) ORDER BY id"
    )


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


@pytest.mark.parametrize("storage_type", ["s3", "local"])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_read_in_order_nulls_first(started_cluster_iceberg_with_spark, storage_type, with_nulls):
    """Spark sorts ASC NULLS FIRST by default, while the in-order merge keeps NULLs last.
    Files with NULL keys must keep the sorting step; files without NULLs still read in order."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = f"test_read_in_order_nulls_first_{storage_type}_{int(with_nulls)}_{get_uuid_str()}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id BIGINT, data STRING) USING iceberg")
    spark.sql(f"ALTER TABLE {TABLE_NAME} WRITE ORDERED BY id ASC NULLS FIRST")

    null = "NULL" if with_nulls else "7"
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({null}, 'a'), (3, 'c'), (5, 'e')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'b'), ({null}, 'd'), (2, 'f')")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query = f"SELECT id FROM {TABLE_NAME} ORDER BY id"
    expected = instance.query(f"{query} SETTINGS optimize_read_in_order=0")
    assert expected.split() == (["1", "2", "3", "5", "\\N", "\\N"] if with_nulls else ["1", "2", "3", "5", "7", "7"])
    for max_threads in [1, 4]:
        assert instance.query(f"{query} SETTINGS max_threads={max_threads}") == expected

    assert (count_in_pipeline(instance, query, "PartialSortingTransform") > 0) == with_nulls


def test_read_in_order_cloned_read(started_cluster_iceberg_with_spark):
    """A sealed view clones its optimized plan, and a correlated subquery executes it twice;
    each copy must list its own files, whether reading in order was declined or accepted."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_cloned_read_" + get_uuid_str()

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id BIGINT) USING iceberg")
    spark.sql(f"ALTER TABLE {TABLE_NAME} WRITE ORDERED BY id")
    for i in range(1, 7):
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({i}), ({i + 6})")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # Six files over a threshold of one decline; a correlated subquery executes the copies one after another.
    instance.query(
        f"CREATE VIEW {TABLE_NAME}_declined SQL SECURITY NONE AS SELECT id FROM {TABLE_NAME} ORDER BY id "
        f"SETTINGS max_threads=1, read_in_order_two_level_merge_threshold=1")
    query = (
        f"SELECT id FROM {TABLE_NAME}_declined AS o WHERE EXISTS (SELECT 1 FROM numbers(20) WHERE number = o.id) "
        f"ORDER BY id SETTINGS correlated_subqueries_use_in_memory_buffer=0")
    assert get_array_as_returned(instance.query(query)) == list(range(1, 13))
    assert get_array_as_returned(instance.query(f"{query}, optimize_read_in_order=0")) == list(range(1, 13))

    # A sealed view takes optimization settings from the outer query; a self-join runs both accepted copies at once.
    instance.query(f"CREATE VIEW {TABLE_NAME}_accepted SQL SECURITY NONE AS SELECT id, count() AS c FROM {TABLE_NAME} GROUP BY id")
    query = (
        f"SELECT a.id, b.c FROM {TABLE_NAME}_accepted AS a INNER JOIN {TABLE_NAME}_accepted AS b ON a.id = b.id ORDER BY a.id "
        f"SETTINGS join_algorithm='full_sorting_merge', max_threads=4")
    copies = instance.query(f"EXPLAIN PIPELINE {query}, optimize_aggregation_in_order=1").split("(ReadFromSealedView)")[1:]
    assert len(copies) == 2 and all("AggregatingInOrderTransform" in copy for copy in copies), copies
    assert instance.query(f"{query}, optimize_aggregation_in_order=1") == instance.query(
        f"{query}, optimize_read_in_order=0, optimize_aggregation_in_order=0")


def test_read_in_order_several_row_groups(started_cluster_iceberg_with_spark):
    """With the sort elided, a file's row groups must arrive in file order. Roaring-bitmap position
    deletes stop the Iceberg reader from pinning preserve_order itself, so only the in-order read pins it."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_row_groups_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id BIGINT) USING iceberg "
        f"TBLPROPERTIES ('write.parquet.row-group-size-bytes' = '1')")
    spark.sql(f"ALTER TABLE {TABLE_NAME} WRITE LOCALLY ORDERED BY id")
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT /*+ COALESCE(1) */ id FROM range(0, 10000)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    row_groups = instance.query(
        f"SELECT num_row_groups FROM file('iceberg_data/default/{TABLE_NAME}/data/**.parquet', 'ParquetMetadata')")
    assert len(row_groups.split()) == 1 and int(row_groups) > 10, row_groups

    query = f"SELECT id FROM {TABLE_NAME} ORDER BY id"
    settings = "max_threads=8, input_format_parquet_preserve_order=0, use_roaring_bitmap_iceberg_positional_deletes=1"
    assert count_in_pipeline(instance, query, "PartialSortingTransform", settings) == 0
    assert count_in_pipeline(instance, query, "MergeSortingTransform", settings) == 0
    for _ in range(5):
        assert get_array_as_returned(instance.query(f"{query} SETTINGS {settings}")) == list(range(10000))


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

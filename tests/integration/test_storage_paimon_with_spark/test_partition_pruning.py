import pytest

from helpers.paimon_utils import (
    create_spark_paimon_table,
    get_creation_expression,
    get_uuid_str,
    query_profile_events,
    spark_table,
    upload_paimon_table,
)


def _read_files_count(instance, query, settings):
    # The query condition cache would skip the files an earlier query with the same predicate found
    # to have no match, which is not the pruning this counts.
    return query_profile_events(
        instance,
        query,
        ["EngineFileLikeReadFiles"],
        settings={**settings, "use_query_condition_cache": 0},
    )[0]


def _check_pruning(instance, table_expr, predicate, expected_pruned_files):
    select_query = f"SELECT * FROM {table_expr} WHERE {predicate} ORDER BY ALL"

    result_unpruned = instance.query(
        select_query, settings={"use_paimon_partition_pruning": 0}
    )
    result_pruned = instance.query(
        select_query, settings={"use_paimon_partition_pruning": 1}
    )
    assert result_unpruned == result_pruned

    files_unpruned = _read_files_count(
        instance,
        f"SELECT * FROM {table_expr} WHERE {predicate} FORMAT Null",
        {"use_paimon_partition_pruning": 0},
    )
    files_pruned = _read_files_count(
        instance,
        f"SELECT * FROM {table_expr} WHERE {predicate} FORMAT Null",
        {"use_paimon_partition_pruning": 1},
    )
    assert files_pruned == expected_pruned_files
    assert files_pruned < files_unpruned


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_partition_pruning(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_pruning_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(
        spark, table_name, "id BIGINT, val STRING, part INT", partitioned_by="part"
    )
    # Two inserts -> two data files per partition, 4 partitions, 8 files total.
    for i in range(2):
        spark.sql(
            f"INSERT INTO {spark_table(table_name)} "
            f"SELECT id + {i * 100}, concat('v', id), id % 4 FROM range(100)"
        )
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    _check_pruning(instance, table_expr, "part = 2", expected_pruned_files=2)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_partition_pruning_types(started_cluster_paimon_with_spark, storage_type):
    """Non-integer partition values: their directory names are encoded in object
    storage paths, where escaping bugs hide."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    # DATE partitions, equality and range predicates.
    table_name = "test_pruning_date_" + storage_type + "_" + get_uuid_str()
    create_spark_paimon_table(
        spark, table_name, "id BIGINT, part DATE", partitioned_by="part"
    )
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} VALUES "
        f"(1, DATE '2024-01-01'), (2, DATE '2024-01-02'), (3, DATE '2024-01-03')"
    )
    upload_paimon_table(started_cluster, storage_type, table_name)
    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    _check_pruning(instance, table_expr, "part = '2024-01-02'", expected_pruned_files=1)
    _check_pruning(instance, table_expr, "part >= '2024-01-02'", expected_pruned_files=2)

    # STRING partitions with characters that require path escaping.
    table_name = "test_pruning_string_" + storage_type + "_" + get_uuid_str()
    create_spark_paimon_table(
        spark, table_name, "id BIGINT, part STRING", partitioned_by="part"
    )
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} VALUES "
        f"(1, 'plain'), (2, 'with space'), (3, 'with%percent')"
    )
    upload_paimon_table(started_cluster, storage_type, table_name)
    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    assert (
        instance.query(
            f"SELECT id FROM {table_expr} WHERE part = 'with space'",
            settings={"use_paimon_partition_pruning": 1},
        )
        == "2\n"
    )
    _check_pruning(instance, table_expr, "part = 'with space'", expected_pruned_files=1)
    _check_pruning(
        instance, table_expr, "part = 'with%percent'", expected_pruned_files=1
    )

    # DECIMAL partitions exercise the BinaryRow decimal decoding of encoded
    # partition values (a documented supported partition key type).
    table_name = "test_pruning_decimal_" + storage_type + "_" + get_uuid_str()
    create_spark_paimon_table(
        spark, table_name, "id BIGINT, part DECIMAL(9, 2)", partitioned_by="part"
    )
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} VALUES "
        f"(1, 1.25), (2, 2.50), (3, 3.75)"
    )
    upload_paimon_table(started_cluster, storage_type, table_name)
    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    assert (
        instance.query(
            f"SELECT id FROM {table_expr} WHERE part = 2.5",
            settings={"use_paimon_partition_pruning": 1},
        )
        == "2\n"
    )
    # A float literal is not prunable against a Decimal partition key; the
    # explicitly-typed literal is.
    _check_pruning(
        instance, table_expr, "part = toDecimal32('2.5', 2)", expected_pruned_files=1
    )

import os

import pytest

from helpers.paimon_utils import (
    PAIMON_WAREHOUSE,
    create_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    query_profile_events,
    spark_table,
    upload_paimon_table,
    wait_until_query_result,
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


def _partition_directories(table_name):
    table_dir = os.path.join(PAIMON_WAREHOUSE, "default.db", table_name)
    return {
        os.path.relpath(root, table_dir)
        for root, dirs, _ in os.walk(table_dir)
        if any(d.startswith("bucket-") for d in dirs)
    }


@pytest.mark.parametrize(
    "columns, partitioned_by, tblproperties, values, expected_dirs, expected",
    [
        pytest.param(
            "part STRING",
            "part",
            {},
            "(1, 'a'), (2, ''), (3, ' '), (4, NULL)",
            {"part=a", "part=__DEFAULT_PARTITION__"},
            "1\ta\n2\t\n3\t \n4\t\\N\n",
            id="blank_string",
        ),
        pytest.param(
            "part STRING",
            "part",
            {"partition.default-name": "EMPTY"},
            "(1, '')",
            {"part=EMPTY"},
            "1\t\n",
            id="custom_default_name",
        ),
        pytest.param(
            "part DOUBLE",
            "part",
            {},
            "(1, 2.0D), (2, 1.0E10D), (3, 1.0E-4D)",
            {"part=2.0", "part=1.0E10", "part=1.0E-4"},
            "1\t2\n2\t10000000000\n3\t0.0001\n",
            id="double",
        ),
        pytest.param(
            "part FLOAT",
            "part",
            {},
            "(1, CAST(2.0 AS FLOAT)), (2, CAST(1.0E10 AS FLOAT)), (3, CAST(1.0E-4 AS FLOAT))",
            {"part=2.0", "part=1.0E10", "part=1.0E-4"},
            "1\t2\n2\t10000000000\n3\t0.0001\n",
            id="float",
        ),
        pytest.param(
            "d DATE, ts TIMESTAMP_NTZ, tl TIMESTAMP, b BOOLEAN, s STRING",
            "d, ts, tl, b, s",
            {"partition.legacy-name": "false"},
            "(1, DATE '2024-01-01', TIMESTAMP_NTZ '2024-01-15 11:30:00.123456', TIMESTAMP '2024-01-15 10:30:00', true, '')",
            {
                "d=2024-01-01/ts=2024-01-15 11%3A30%3A00.123456/tl=2024-01-15 10%3A30%3A00.000000/b=true/s=__DEFAULT_PARTITION__"
            },
            "1\t2024-01-01\t2024-01-15 11:30:00.123456\t2024-01-15 10:30:00.000000\ttrue\t\n",
            id="legacy_name_false",
        ),
    ],
)
def test_partition_directory_names(
    started_cluster_paimon_with_spark,
    columns,
    partitioned_by,
    tblproperties,
    values,
    expected_dirs,
    expected,
):
    """The data file path is rebuilt from the partition value, so it must name the directory Paimon wrote."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_partition_names_" + get_uuid_str()

    create_spark_paimon_table(
        spark,
        table_name,
        "id INT NOT NULL, " + columns,
        partitioned_by=partitioned_by,
        tblproperties=tblproperties,
    )
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES {values}")
    assert _partition_directories(table_name) == expected_dirs
    upload_paimon_table(started_cluster, "s3", table_name)

    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )
    # A local time zone timestamp is rendered in the server time zone, not in the session one.
    assert (
        instance.query(
            f"SELECT count() FROM {table_expr}",
            settings={"session_timezone": "Asia/Shanghai"},
        )
        == f"{expected.count(chr(10))}\n"
    )
    assert (
        instance.query(
            f"SELECT id, {partitioned_by} FROM {table_expr} ORDER BY id"
        )
        == expected
    )


def test_partition_default_name_after_alter(started_cluster_paimon_with_spark):
    """A `Paimon*` table names the default partition with the snapshot's `partition.default-name`."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_partition_default_name_alter_" + get_uuid_str()

    create_spark_paimon_table(
        spark, table_name, "id INT NOT NULL, part STRING", partitioned_by="part"
    )
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'a')")
    upload_paimon_table(started_cluster, "s3", table_name)

    create_paimon_table("s3", instance, table_name, started_cluster)
    with drop_tables_at_exit(instance, table_name):
        assert (
            instance.query(f"SELECT id, part FROM {table_name} ORDER BY id") == "1\ta\n"
        )

        spark.sql(
            f"ALTER TABLE {spark_table(table_name)} SET TBLPROPERTIES ('partition.default-name' = 'EMPTY')"
        )
        spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (2, ''), (3, NULL)")
        assert _partition_directories(table_name) == {"part=a", "part=EMPTY"}
        upload_paimon_table(started_cluster, "s3", table_name)
        wait_until_query_result(
            instance,
            f"SELECT id, part FROM {table_name} ORDER BY id",
            "1\ta\n2\t\n3\t\\N\n",
        )

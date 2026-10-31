import pytest

from helpers.paimon_utils import (
    create_paimon_table,
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    prepare_simple_paimon_table,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_engine_experimental_gate(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_engine_gate_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    create_query = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=False
    )

    error = instance.query_and_get_error(create_query)
    assert "allow_experimental_paimon_storage_engine" in error

    instance.query(
        create_query, settings={"allow_experimental_paimon_storage_engine": 1}
    )
    with drop_tables_at_exit(instance, table_name):
        # Reads do not require the experimental setting, only CREATE does.
        assert instance.query(f"SELECT count() FROM {table_name}") == "2\n"

        # A persisted experimental engine table must load after server restart.
        instance.restart_clickhouse()
        assert instance.query(f"SELECT count() FROM {table_name}") == "2\n"


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_engine_select(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_engine_select_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    table_function_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    expected = instance.query(f"SELECT a, b FROM {table_function_expr} ORDER BY a")

    create_paimon_table(storage_type, instance, table_name, started_cluster)
    with drop_tables_at_exit(instance, table_name):
        assert instance.query(f"SELECT a, b FROM {table_name} ORDER BY a") == expected


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_engine_detach_attach(started_cluster_paimon_with_spark, storage_type):
    """Explicit DETACH/ATTACH round-trip (restart survival covers the
    attach-on-boot path; this covers the statement path)."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_engine_detach_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    create_paimon_table(storage_type, instance, table_name, started_cluster)
    with drop_tables_at_exit(instance, table_name):
        assert instance.query(f"SELECT count() FROM {table_name}") == "2\n"

        instance.query(f"DETACH TABLE {table_name}")
        assert (
            table_name
            not in instance.query(f"SHOW TABLES LIKE '{table_name}'")
        )
        instance.query(f"ATTACH TABLE {table_name}")
        assert instance.query(f"SELECT count() FROM {table_name}") == "2\n"


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_parallel_replicas_read(started_cluster_paimon_with_spark, storage_type):
    """Enabling parallel replicas must not break or corrupt Paimon reads.
    Whether the read is actually distributed is version- and setting-
    dependent; the customer-facing contract asserted here is result
    correctness with the feature enabled."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_parallel_replicas_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT a, b FROM {table_expr} ORDER BY a"
    expected = instance.query(select_query)
    assert (
        instance.query(
            select_query,
            settings={
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 2,
                "cluster_for_parallel_replicas": "cluster_simple",
            },
        )
        == expected
    )


def test_paimon_engine_autodetect(started_cluster_paimon_with_spark):
    """The bare `Paimon` engine: default dispatch resolves to S3, and the
    `disk` setting selects the backend from the disk's object storage type."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_engine_autodetect_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, "s3", table_name)

    # Default dispatch (no disk setting): Paimon(...) == PaimonS3(...).
    create_paimon_table(
        "s3", instance, table_name, started_cluster, engine_name="Paimon"
    )
    with drop_tables_at_exit(instance, table_name):
        assert instance.query(f"SELECT a, b FROM {table_name} ORDER BY a") == "1\tone\n2\ttwo\n"

    # Disk-based dispatch: the S3 disk endpoint already points at .../paimon_data/default.db/.
    instance.query(
        f"""
        CREATE TABLE {table_name}
        ENGINE=Paimon('{table_name}/')
        SETTINGS disk = 'disk_s3_paimon'
        """,
        settings={"allow_experimental_paimon_storage_engine": 1},
    )
    with drop_tables_at_exit(instance, table_name):
        assert instance.query(f"SELECT a, b FROM {table_name} ORDER BY a") == "1\tone\n2\ttwo\n"

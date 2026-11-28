import pytest

from helpers.paimon_utils import (
    assert_cluster_fan_out,
    get_creation_expression,
    get_uuid_str,
    prepare_simple_paimon_table,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_table_function_select(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_tf_select_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(
        started_cluster, storage_type, table_name, with_second_commit=True
    )

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT a, b FROM {table_expr} ORDER BY a"
    assert instance.query(select_query) == "1\tone\n2\ttwo\n3\tthree\n"
    assert instance.query(f"SELECT count() FROM {table_expr}") == "3\n"

    if storage_type == "s3":
        # `paimon` is an alias of `paimonS3`.
        alias_query = select_query.replace("paimonS3(", "paimon(", 1)
        assert instance.query(alias_query) == instance.query(select_query)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_cluster_table_function(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_tf_cluster_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(
        started_cluster, storage_type, table_name, with_second_commit=True
    )

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    cluster_table_expr = get_creation_expression(
        storage_type,
        table_name,
        started_cluster,
        table_function=True,
        run_on_cluster=True,
    )

    expected = instance.query(f"SELECT a, b FROM {table_expr} ORDER BY a")
    query_id = f"paimon-cluster-{get_uuid_str()}"
    assert (
        instance.query(
            f"SELECT a, b FROM {cluster_table_expr} ORDER BY a", query_id=query_id
        )
        == expected
    )
    # Result equality alone cannot tell a real fan-out from a silent fallback
    # to the initiator-only path: the remote replica must have participated.
    assert_cluster_fan_out(started_cluster.instances["node2"], query_id)

    if storage_type == "s3":
        # `paimonCluster` is an alias of `paimonS3Cluster` — and it must fan
        # out itself, not resolve to an initiator-only local read.
        alias_expr = cluster_table_expr.replace("paimonS3Cluster(", "paimonCluster(", 1)
        alias_query_id = f"paimon-cluster-alias-{get_uuid_str()}"
        assert (
            instance.query(
                f"SELECT a, b FROM {alias_expr} ORDER BY a", query_id=alias_query_id
            )
            == expected
        )
        assert_cluster_fan_out(started_cluster.instances["node2"], alias_query_id)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_virtual_columns(started_cluster_paimon_with_spark, storage_type):
    """_path/_file/_size are documented for the paimon table functions; they
    are populated from object storage paths, where backend bugs hide."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_tf_virtual_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(
        started_cluster, storage_type, table_name, with_second_commit=True
    )

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    rows = instance.query(
        f"SELECT DISTINCT _path, _file, _size > 0 FROM {table_expr}"
    ).strip().split("\n")
    # Two inserts -> two data files.
    assert len(rows) == 2
    for row in rows:
        path, file, size_positive = row.split("\t")
        assert f"paimon_data/default.db/{table_name}" in path
        assert file.endswith(".parquet")
        assert size_positive == "1"


@pytest.mark.xfail(
    reason="https://github.com/ClickHouse/ClickHouse/issues/113361: the documented "
    "structure argument is reported by DESC but ignored by SELECT",
    strict=True,
)
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_explicit_structure(started_cluster_paimon_with_spark, storage_type):
    """Asserts the DOCUMENTED contract — the structure argument overrides the
    inferred schema — as a strict xfail (same pattern as the primary-key
    test): it flips to a required test when ClickHouse#113361 is fixed,
    instead of pinning the current bug on the success path."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_tf_structure_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(
        started_cluster, storage_type, table_name, with_second_commit=True
    )

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    structured_expr = table_expr[:-1] + ", structure = 'a Int64, b String')"
    assert instance.query(f"SELECT count() FROM {structured_expr}") == "3\n"
    assert (
        instance.query(f"SELECT toTypeName(a) FROM {structured_expr} LIMIT 1")
        == "Int64\n"
    )

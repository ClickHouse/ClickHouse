"""RESTORE of `system.functions` accepts SQL user-defined functions created by an older version,
including those that the current `CREATE FUNCTION` rejects."""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# 25.3 still accepts every definition in LEGACY_FUNCTIONS.
node = cluster.add_instance(
    "node",
    main_configs=["configs/backups.xml"],
    image="clickhouse/clickhouse-server",
    tag="25.3",
    stay_alive=True,
    with_installed_binary=True,
)

LEGACY_FUNCTIONS = {
    "udf_not_lambda": "JSONExtractString((x, y), x)",
    "udf_identity_arguments": "lambda(identity(x), x)",
    "udf_parametric_arguments": "lambda(quantile(0.5)(x), x)",
}
FUNCTIONS = {**LEGACY_FUNCTIONS, "udf_lambda": "(x, y) -> (x + y)"}

LIST_FUNCTIONS = "SELECT name, create_query FROM system.functions WHERE origin = 'SQLUserDefined' ORDER BY name"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    drop_functions()
    node.restart_with_original_version(clear_data_dir=True)


def drop_functions():
    for name in FUNCTIONS:
        node.query(f"DROP FUNCTION IF EXISTS {name}")


def new_backup(prefix):
    return f"File('/var/lib/clickhouse/backups/{prefix}_{uuid.uuid4().hex}')"


def test_restore_functions_created_by_older_version(start_cluster):
    for name, definition in FUNCTIONS.items():
        node.query(f"CREATE FUNCTION {name} AS {definition}")
    old_backup = new_backup("old")
    node.query(f"BACKUP TABLE system.functions TO {old_backup}")

    node.restart_with_latest_version()

    expected = node.query(LIST_FUNCTIONS)
    assert [row.split("\t")[0] for row in expected.splitlines()] == sorted(FUNCTIONS)

    node.query(f"RESTORE TABLE system.functions FROM {old_backup}")
    assert node.query(LIST_FUNCTIONS) == expected

    drop_functions()
    node.query(f"RESTORE TABLE system.functions FROM {old_backup}")
    assert node.query(LIST_FUNCTIONS) == expected

    backup = new_backup("new")
    node.query(f"BACKUP TABLE system.functions TO {backup}")
    drop_functions()
    node.query(f"RESTORE TABLE system.functions FROM {backup}")
    assert node.query(LIST_FUNCTIONS) == expected

    assert node.query("SELECT udf_lambda(1, 2)") == "3\n"

    for name, definition in LEGACY_FUNCTIONS.items():
        assert "BAD_ARGUMENTS" in node.query_and_get_error(
            f"CREATE FUNCTION {name}_new AS {definition}"
        )

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import (
    get_database_disk_name,
    read_metadata,
    write_metadata,
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
# Same server, except that its data directory is deep enough that a part's projection subdirectory
# does not fit PATH_MAX even when the projection name itself fits one path component.
deep_node = cluster.add_instance(
    "deep_node", main_configs=["configs/deep_path.xml"], stay_alive=True
)

# Over the limit, and the length from the original report. A fresh DDL rejects it, so the only way to
# get such a table is to plant the definition into stored metadata, which is how a table created
# before the check existed looks on disk.
LEGACY_NAME = "p" * 251

# The largest accepted name, so every form derived from it fits one path component and nothing here is
# the projection's fault.
AT_LIMIT_NAME = "w" * 214


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def plant_over_limit_projection(table):
    """Give `table` a projection name that a fresh DDL would reject.

    Only the stored metadata .sql is edited, through the database disk; part directories are never
    touched.
    """
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree ORDER BY a")
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(3)")

    metadata_path = node.query(
        f"SELECT metadata_path FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()

    node.query(f"DETACH TABLE {table}")
    definition = read_metadata(node, metadata_path)
    anchor = "`a` UInt64\n)"
    assert anchor in definition, definition
    write_metadata(
        node,
        metadata_path,
        definition.replace(
            anchor,
            "`a` UInt64,\n    PROJECTION `%s`\n    (\n        SELECT a\n        ORDER BY a\n    )\n)"
            % LEGACY_NAME,
        ),
    )
    db_disk_name = get_database_disk_name(node)
    if db_disk_name != "default":
        node.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")
    # The definition is only read at load time, so restart rather than ATTACH in place.
    node.restart_clickhouse()


def test_legacy_over_limit_projection_reports_the_limit():
    """A stored definition whose projection name no longer fits a path component keeps loading, and
    writing reports the limit instead of leaking the filesystem error."""
    table = "legacy_projection"
    plant_over_limit_projection(table)

    assert node.query(f"SELECT count() FROM {table}").strip() == "3"
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
        ).strip()
        == "1"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.detached_parts WHERE table = '{table}'"
        ).strip()
        == "0"
    )

    error = node.query_and_get_error(f"INSERT INTO {table} SELECT 9")
    assert "ARGUMENT_OUT_OF_BOUND" in error, error
    assert "The max length of projection name is 214" in error, error
    assert LEGACY_NAME in error, error

    # Recoverable: dropping the projection restores writes.
    node.query(f"ALTER TABLE {table} DROP PROJECTION `{LEGACY_NAME}`")
    node.query(f"INSERT INTO {table} SELECT 9")
    assert node.query(f"SELECT count() FROM {table}").strip() == "4"

    node.query(f"DROP TABLE {table} SYNC")


def test_legacy_over_limit_projection_is_add_time_business():
    """Re-declaring a name that is already present is `add`'s answer to give, so the length check must
    not pre-empt it: `IF NOT EXISTS` stays a no-op and a duplicate still reports that it exists. Needs
    a name that is over the limit, which only a planted definition has."""
    table = "legacy_duplicate"
    plant_over_limit_projection(table)

    node.query(
        f"ALTER TABLE {table} ADD PROJECTION IF NOT EXISTS `{LEGACY_NAME}` (SELECT a ORDER BY a)"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.projections WHERE table = '{table}'"
        ).strip()
        == "1"
    )

    error = node.query_and_get_error(
        f"ALTER TABLE {table} ADD PROJECTION `{LEGACY_NAME}` (SELECT a ORDER BY a)"
    )
    assert "ILLEGAL_PROJECTION" in error, error
    assert "max length of projection name" not in error, error

    node.query(f"DROP TABLE {table} SYNC")


def test_create_as_a_legacy_source_is_rejected():
    """`CREATE TABLE ... AS <source>` re-materializes the source definition into a fresh create query,
    so the copy is new user input and is refused. The source itself stays readable."""
    table = "legacy_source"
    plant_over_limit_projection(table)

    error = node.query_and_get_error(f"CREATE TABLE legacy_copy AS {table}")
    assert "ARGUMENT_OUT_OF_BOUND" in error, error
    assert "The max length of projection name is 214" in error, error

    assert node.query(f"SELECT count() FROM {table}").strip() == "3"

    node.query(f"DROP TABLE {table} SYNC")


def test_deep_data_root_keeps_its_own_error():
    """A whole path over PATH_MAX raises the same ENAMETOOLONG, but it is not the projection's fault:
    the name here fits one path component. The translation must decline so the operator sees the real
    error instead of being told to rename a projection that is already within the limit.
    """
    deep_node.query("DROP TABLE IF EXISTS deep_t SYNC")
    deep_node.query(
        f"CREATE TABLE deep_t (a UInt64, "
        f"PROJECTION `{AT_LIMIT_NAME}` (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a"
    )

    error = deep_node.query_and_get_error(
        "INSERT INTO deep_t SELECT number FROM numbers(3)"
    )
    assert "File name too long" in error, error
    assert "max length of projection name" not in error, error
    assert "ARGUMENT_OUT_OF_BOUND" not in error, error

    deep_node.query("DROP TABLE deep_t SYNC")

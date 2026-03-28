import pytest
import re
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    macros={"replica": "r1"},
    main_configs=["config/config.xml", "config/config1.xml"],
)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    macros={"replica": "r2"},
    main_configs=["config/config.xml", "config/config2.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def verify_warning_with_values(node, current_val, warn_val, throw_val):
    """
    Parses system.warnings to find a warning message matching the expected values.
    """
    warnings_result = node.query("SELECT message, message_format_string FROM system.warnings")
    rows = warnings_result.strip().split('\n')

    found = False
    found_message = ""

    for row in rows:
        if not row: continue
        parts = row.split('\t')
        if len(parts) < 1: continue

        message = parts[0]

        # Regex to capture values from message:
        match = re.search(
            r"\((\d+)\) exceeds the warning limit of (\d+)(?:\. You will not be able to create new .* limit of (\d+) is reached)?",
            message)

        if match:
            msg_curr = int(match.group(1))
            msg_warn = int(match.group(2))
            msg_throw = int(match.group(3)) if match.group(3) else None

            # We allow current_val to be >= expected because some internal system objects might have been attached
            if msg_curr >= current_val and msg_warn == warn_val and msg_throw == throw_val:
                found = True
                found_message = message
                break

    assert found, (
        f"Warning not found or values mismatch.\n"
        f"Expected: Current>={current_val}, Warn={warn_val}, Throw={throw_val}\n"
        f"Last found message: {found_message}"
    )


def verify_no_warning(node, message_part):
    """
    Verifies that no warning containing message_part exists in system.warnings.
    """
    warnings = node.query("SELECT message FROM system.warnings").strip()
    if not warnings:
        return
    assert message_part not in warnings, f"Found unexpected warning containing '{message_part}':\n{warnings}"


def test_table_limit(started_cluster):
    warn_limit = 5
    throw_limit = 10

    for i in range(15):
        node.query(f"DROP TABLE IF EXISTS t{i} SYNC")

    for i in range(warn_limit):
        node.query(f"CREATE TABLE t{i} (a Int32) Engine = Log")

    node.query(f"CREATE TABLE t{warn_limit} (a Int32) Engine = Log")

    verify_warning_with_values(
        node,
        current_val=warn_limit + 1,
        warn_val=warn_limit,
        throw_val=throw_limit
    )

    for i in range(warn_limit + 1, throw_limit):
        node.query(f"CREATE TABLE t{i} (a Int32) Engine = Log")

    verify_warning_with_values(
        node,
        current_val=throw_limit,
        warn_val=warn_limit,
        throw_val=throw_limit
    )

    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        f"CREATE TABLE t{throw_limit} (a Int32) Engine = Log"
    )

    # Cleanup and check warning disappears
    for i in range(throw_limit):
        node.query(f"DROP TABLE t{i} SYNC")

    verify_no_warning(node, "The number of attached tables")


def test_view_limit(started_cluster):
    warn_limit = 5
    throw_limit = 10

    for i in range(warn_limit + 1):
        node.query(f"CREATE VIEW v{i} AS SELECT 1")

    verify_warning_with_values(
        node,
        current_val=warn_limit + 1,
        warn_val=warn_limit,
        throw_val=throw_limit
    )

    for i in range(warn_limit + 1, throw_limit):
        node.query(f"CREATE VIEW v{i} AS SELECT 1")

    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        f"CREATE VIEW v{throw_limit} AS SELECT 1"
    )

    # Cleanup and check warning disappears
    for i in range(throw_limit):
        node.query(f"DROP VIEW v{i}")

    verify_no_warning(node, "The number of attached views")


def test_database_limit(started_cluster):
    warn_limit = 5
    throw_limit = 10
    created_dbs = []

    try:
        initial_dbs = int(node.query("SELECT value FROM system.metrics WHERE name = 'AttachedDatabase'"))

        # 1. Reach Warning Limit
        dbs_to_warn = max(0, warn_limit - initial_dbs + 1)
        for i in range(dbs_to_warn):
            name = f"db_test_{i}"
            node.query(f"CREATE DATABASE {name}")
            created_dbs.append(name)

        current_total = int(node.query("SELECT value FROM system.metrics WHERE name = 'AttachedDatabase'"))

        verify_warning_with_values(
            node,
            current_val=current_total,
            warn_val=warn_limit,
            throw_val=throw_limit
        )

        # 2. Reach Throw Limit (and try to exceed)
        limit_hit = False

        # Try creating enough databases to definitely hit the limit + safety margin
        for i in range(len(created_dbs), throw_limit + 10):
            name = f"db_test_{i}"
            try:
                node.query(f"CREATE DATABASE {name}")
                created_dbs.append(name)
            except QueryRuntimeException as e:
                if "TOO_MANY_DATABASES" in str(e):
                    limit_hit = True
                    break
                else:
                    raise e

        assert limit_hit, f"Failed to trigger TOO_MANY_DATABASES limit. Created {len(created_dbs)} databases on top of {initial_dbs} initial."

        # Cleanup and verify warning disappears
        for db in created_dbs:
            node.query(f"DROP DATABASE IF EXISTS {db}")

        verify_no_warning(node, "The number of attached databases")
        created_dbs = []  # Clear list so finally block doesn't duplicate effort

    finally:
        for db in created_dbs:
            node.query(f"DROP DATABASE IF EXISTS {db}")


def test_dictionary_limit(started_cluster):
    warn_limit = 5
    throw_limit = 10

    for i in range(warn_limit + 1):
        node.query(f"CREATE DICTIONARY d{i} (a Int32) primary key a source(null()) layout(flat()) lifetime(1000)")

    verify_warning_with_values(
        node,
        current_val=warn_limit + 1,
        warn_val=warn_limit,
        throw_val=throw_limit
    )

    for i in range(warn_limit + 1, throw_limit):
        node.query(f"CREATE DICTIONARY d{i} (a Int32) primary key a source(null()) layout(flat()) lifetime(1000)")

    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        f"CREATE DICTIONARY d{throw_limit} (a Int32) primary key a source(null()) layout(flat()) lifetime(1000)"
    )

    # Cleanup and check warning disappears
    for i in range(throw_limit):
        node.query(f"DROP DICTIONARY d{i}")

    verify_no_warning(node, "The number of attached dictionaries")


def test_named_collection_limit(started_cluster):
    warn_limit = 5
    throw_limit = 10

    def _get_number_of_collections():
        return int(node.query("SELECT value FROM system.metrics WHERE name = 'NamedCollection'"))

    try:
        for i in range(warn_limit):
            node.query(f"CREATE NAMED COLLECTION nc_{i} AS key=1")

        node.query(f"CREATE NAMED COLLECTION nc_{warn_limit} AS key=1")

        verify_warning_with_values(
            node,
            current_val=warn_limit + 1,
            warn_val=warn_limit,
            throw_val=throw_limit
        )

        for i in range(warn_limit + 1, throw_limit):
            node.query(f"CREATE NAMED COLLECTION nc_{i} AS key=1")

        assert _get_number_of_collections() == throw_limit

        assert "TOO_MANY_NAMED_COLLECTIONS" in node.query_and_get_error(
            f"CREATE NAMED COLLECTION nc_{throw_limit} AS key=1"
        )

        node.query(f"DROP NAMED COLLECTION IF EXISTS nc_1")

        verify_warning_with_values(
            node,
            current_val=throw_limit-1,
            warn_val=warn_limit,
            throw_val=throw_limit
        )

        node.query(f"DROP NAMED COLLECTION IF EXISTS nc_1")

        verify_warning_with_values(
            node,
            current_val=throw_limit-1,
            warn_val=warn_limit,
            throw_val=throw_limit
        )

        # Cleanup and check warning disappears
        for i in range(throw_limit + 1):
            node.query(f"DROP NAMED COLLECTION IF EXISTS nc_{i}")

        verify_no_warning(node, "The number of named collections")

    finally:
        for i in range(throw_limit + 1):
            node.query(f"DROP NAMED COLLECTION IF EXISTS nc_{i}")

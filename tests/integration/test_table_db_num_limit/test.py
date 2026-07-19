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
    main_configs=["config/config.xml", "config/config2.xml", "config/named_collections.xml"],
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

        node.query("DROP NAMED COLLECTION IF EXISTS nc_1")

        verify_warning_with_values(
            node,
            current_val=throw_limit-1,
            warn_val=warn_limit,
            throw_val=throw_limit
        )

        node.query("DROP NAMED COLLECTION IF EXISTS nc_1")

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


def test_named_collection_if_not_exists_metric(started_cluster):
    """
    CREATE NAMED COLLECTION IF NOT EXISTS must not inflate the NamedCollection
    metric when the collection already exists.
    DROP NAMED COLLECTION IF EXISTS on a nonexistent collection must not
    deflate the metric either.
    https://github.com/ClickHouse/ClickHouse/issues/102507
    """

    def _get_number_of_collections():
        return int(node.query("SELECT value FROM system.metrics WHERE name = 'NamedCollection'"))

    try:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_ine_test")
        before = _get_number_of_collections()

        node.query("CREATE NAMED COLLECTION nc_ine_test AS key = 1")
        assert _get_number_of_collections() == before + 1

        # IF NOT EXISTS on an already-existing collection must not change the metric
        node.query("CREATE NAMED COLLECTION IF NOT EXISTS nc_ine_test AS key = 2")
        assert _get_number_of_collections() == before + 1

        node.query("CREATE NAMED COLLECTION IF NOT EXISTS nc_ine_test AS key = 3")
        assert _get_number_of_collections() == before + 1

        node.query("DROP NAMED COLLECTION nc_ine_test")
        assert _get_number_of_collections() == before

        # DROP IF EXISTS on an already-dropped collection must not change the metric
        node.query("DROP NAMED COLLECTION IF EXISTS nc_ine_test")
        assert _get_number_of_collections() == before

        node.query("DROP NAMED COLLECTION IF EXISTS nc_ine_test")
        assert _get_number_of_collections() == before

    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_ine_test")


def test_named_collection_metric_on_startup(started_cluster):
    """
    Collections loaded from config on startup must be reflected in the
    NamedCollection metric. node2 starts with 2 config-defined collections
    (from_config_1, from_config_2).
    https://github.com/ClickHouse/ClickHouse/issues/102507
    """

    def _get_metric(n):
        return int(n.query("SELECT value FROM system.metrics WHERE name = 'NamedCollection'"))

    metric = _get_metric(node2)
    assert metric >= 2, (
        f"Expected NamedCollection metric >= 2 (from_config_1 + from_config_2), got {metric}"
    )


def test_named_collection_metric_after_config_reload(started_cluster):
    """
    SYSTEM RELOAD CONFIG must not break the NamedCollection metric for
    SQL-created collections.
    https://github.com/ClickHouse/ClickHouse/issues/102507
    """

    def _get_metric(n):
        return int(n.query("SELECT value FROM system.metrics WHERE name = 'NamedCollection'"))

    try:
        node2.query("DROP NAMED COLLECTION IF EXISTS nc_reload_test")
        before = _get_metric(node2)

        node2.query("CREATE NAMED COLLECTION nc_reload_test AS key = 1")
        assert _get_metric(node2) == before + 1

        node2.query("SYSTEM RELOAD CONFIG")
        assert _get_metric(node2) == before + 1

        node2.query("DROP NAMED COLLECTION nc_reload_test")
        assert _get_metric(node2) == before

    finally:
        node2.query("DROP NAMED COLLECTION IF EXISTS nc_reload_test")


@pytest.mark.parametrize(
    "setting_name, initial_limit, lower_limit, metric_name, error_name, kind",
    [
        pytest.param("max_table_num_to_throw", 10, 6, "AttachedTable", "TOO_MANY_TABLES", "table", id="table"),
        pytest.param(
            "max_replicated_table_num_to_throw",
            5,
            2,
            "AttachedReplicatedTable",
            "TOO_MANY_TABLES",
            "replicated_table",
            id="replicated-table",
        ),
        pytest.param("max_view_num_to_throw", 10, 6, "AttachedView", "TOO_MANY_TABLES", "view", id="view"),
        pytest.param(
            "max_dictionary_num_to_throw",
            10,
            6,
            "AttachedDictionary",
            "TOO_MANY_TABLES",
            "dictionary",
            id="dictionary",
        ),
        pytest.param("max_database_num_to_throw", 10, 6, "AttachedDatabase", "TOO_MANY_DATABASES", "database", id="database"),
        pytest.param(
            "max_named_collection_num_to_throw",
            10,
            6,
            "NamedCollection",
            "TOO_MANY_NAMED_COLLECTIONS",
            "named_collection",
            id="named-collection",
        ),
    ],
)
def test_runtime_config_reload_of_to_throw_limits(
    started_cluster,
    setting_name,
    initial_limit,
    lower_limit,
    metric_name,
    error_name,
    kind,
):
    """Runtime reload must affect each `to_throw` limit enforcement path."""

    config_path = "/etc/clickhouse-server/config.d/reload_test.xml"
    max_objects = initial_limit + 2

    def _get_server_setting():
        return int(node.query(f"SELECT value FROM system.server_settings WHERE name = '{setting_name}'").strip())

    def _get_server_setting_function():
        return int(node.query(f"SELECT getServerSetting('{setting_name}')").strip())

    def _get_count_for_limit():
        if kind == "database":
            return int(node.query(
                """
                SELECT count()
                FROM system.databases
                WHERE name NOT IN ('_temporary_and_external_tables', 'system', 'information_schema', 'INFORMATION_SCHEMA')
                """
            ).strip())
        return int(node.query(f"SELECT value FROM system.metrics WHERE name = '{metric_name}'").strip())

    def _reload_without_override():
        node.exec_in_container(["bash", "-c", f"rm -f {config_path}"])
        node.query("SYSTEM RELOAD CONFIG")

    def _reload_with_limit(limit):
        node.replace_config(config_path, f"<clickhouse><{setting_name}>{limit}</{setting_name}></clickhouse>")
        node.query("SYSTEM RELOAD CONFIG")

    def _object_name(i):
        return f"reload_limit_{kind}_{i}"

    def _create(i):
        name = _object_name(i)
        if kind == "table":
            node.query(f"CREATE TABLE {name} (a Int32) ENGINE = Log")
        elif kind == "replicated_table":
            node.query(
                f"CREATE TABLE {name} (a Int32) "
                f"ENGINE = ReplicatedMergeTree('/clickhouse/test_table_db_num_limit/{name}', 'r1') "
                "ORDER BY a"
            )
        elif kind == "view":
            node.query(f"CREATE VIEW {name} AS SELECT 1")
        elif kind == "dictionary":
            node.query(
                f"CREATE DICTIONARY {name} (a Int32) PRIMARY KEY a "
                "SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(1000)"
            )
        elif kind == "database":
            node.query(f"CREATE DATABASE {name}")
        elif kind == "named_collection":
            node.query(f"CREATE NAMED COLLECTION {name} AS key = 1")
        else:
            raise AssertionError(f"Unknown limit kind: {kind}")

    def _create_and_get_error(i):
        name = _object_name(i)
        if kind == "table":
            return node.query_and_get_error(f"CREATE TABLE {name} (a Int32) ENGINE = Log")
        if kind == "replicated_table":
            return node.query_and_get_error(
                f"CREATE TABLE {name} (a Int32) "
                f"ENGINE = ReplicatedMergeTree('/clickhouse/test_table_db_num_limit/{name}', 'r1') "
                "ORDER BY a"
            )
        if kind == "view":
            return node.query_and_get_error(f"CREATE VIEW {name} AS SELECT 1")
        if kind == "dictionary":
            return node.query_and_get_error(
                f"CREATE DICTIONARY {name} (a Int32) PRIMARY KEY a "
                "SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(1000)"
            )
        if kind == "database":
            return node.query_and_get_error(f"CREATE DATABASE {name}")
        if kind == "named_collection":
            return node.query_and_get_error(f"CREATE NAMED COLLECTION {name} AS key = 1")
        raise AssertionError(f"Unknown limit kind: {kind}")

    def _cleanup():
        for i in range(max_objects):
            name = _object_name(i)
            if kind == "table" or kind == "replicated_table":
                node.query(f"DROP TABLE IF EXISTS {name} SYNC")
            elif kind == "view":
                node.query(f"DROP VIEW IF EXISTS {name}")
            elif kind == "dictionary":
                node.query(f"DROP DICTIONARY IF EXISTS {name}")
            elif kind == "database":
                node.query(f"DROP DATABASE IF EXISTS {name}")
            elif kind == "named_collection":
                node.query(f"DROP NAMED COLLECTION IF EXISTS {name}")

    try:
        _cleanup()
        _reload_without_override()

        assert _get_server_setting() == initial_limit
        assert _get_server_setting_function() == initial_limit

        target_limit = max(lower_limit, _get_count_for_limit())
        assert target_limit < initial_limit

        i = 0
        while _get_count_for_limit() < target_limit:
            _create(i)
            i += 1

        _reload_with_limit(target_limit)

        assert _get_server_setting() == target_limit
        assert _get_server_setting_function() == target_limit
        assert error_name in _create_and_get_error(i)

        _reload_without_override()

        assert _get_server_setting() == initial_limit
        assert _get_server_setting_function() == initial_limit

    finally:
        _cleanup()
        _reload_without_override()

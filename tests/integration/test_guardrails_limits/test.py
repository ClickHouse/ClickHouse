import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster.add_instance("node", main_configs=['configs/warnings_config.xml'])


@pytest.fixture(scope="module")
def init_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def node(init_cluster):
    return cluster.instances["node"]


def check_warning(node, message_part, present=True, retries=20):
    for _ in range(retries):
        sql = f"SELECT count() FROM system.warnings WHERE message LIKE '%{message_part}%'"
        try:
            count = int(node.query(sql).strip())
            if present and count > 0:
                return
            if not present and count == 0:
                return
        except Exception as e:
            # Ignored during retries
            pass
        time.sleep(0.5)

    current_warnings = node.query("SELECT name, message FROM system.warnings")
    if present:
        pytest.fail(
            f"Expected warning containing '{message_part}' was not found.\nCurrent warnings:\n{current_warnings}")
    else:
        pytest.fail(
            f"Warning containing '{message_part}' was found but not expected.\nCurrent warnings:\n{current_warnings}")


def test_database_limit_warning(node):
    limit = 5
    # Metric: AttachedDatabase
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'AttachedDatabase'"))

    warning_msg = "The number of attached databases"
    dbs_to_create = []

    try:
        if initial_count <= limit:
            check_warning(node, warning_msg, present=False)

        needed = (limit + 1) - initial_count

        for i in range(needed):
            db_name = f"warn_db_{i}"
            node.query(f"CREATE DATABASE {db_name}")
            dbs_to_create.append(db_name)

        check_warning(node, warning_msg, present=True)

    finally:
        for db in dbs_to_create:
            node.query(f"DROP DATABASE IF EXISTS {db}")

    check_warning(node, warning_msg, present=False)


def test_view_limit_warning(node):
    limit = 5
    # Metric: AttachedView
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'AttachedView'"))
    warning_msg = "The number of attached views"
    views_to_create = []

    try:
        if initial_count <= limit:
            check_warning(node, warning_msg, present=False)

        needed = (limit + 1) - initial_count

        for i in range(needed):
            view_name = f"default.warn_view_{i}"
            node.query(f"CREATE VIEW {view_name} AS SELECT 1")
            views_to_create.append(view_name)

        check_warning(node, warning_msg, present=True)

    finally:
        for view in views_to_create:
            node.query(f"DROP VIEW IF EXISTS {view}")

    check_warning(node, warning_msg, present=False)


def test_dictionary_limit_warning(node):
    limit = 5
    # Metric: AttachedDictionary
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'AttachedDictionary'"))
    warning_msg = "The number of attached dictionaries"
    dicts_to_create = []

    try:
        if initial_count <= limit:
            check_warning(node, warning_msg, present=False)

        needed = (limit + 1) - initial_count

        for i in range(needed):
            dict_name = f"default.warn_dict_{i}"
            node.query(f"""
                CREATE DICTIONARY {dict_name} (id UInt64, val UInt64)
                PRIMARY KEY id
                SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'system.numbers' DB 'system'))
                LAYOUT(FLAT())
                LIFETIME(MIN 0 MAX 1000)
            """)
            dicts_to_create.append(dict_name)

        check_warning(node, warning_msg, present=True)

    finally:
        for d in dicts_to_create:
            node.query(f"DROP DICTIONARY IF EXISTS {d}")

    check_warning(node, warning_msg, present=False)


def test_table_limit_warning(node):
    limit = 5
    # Metric: AttachedTable
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'AttachedTable'"))
    warning_msg = "The number of attached tables"
    tables_to_create = []

    try:
        if initial_count <= limit:
            check_warning(node, warning_msg, present=False)

        needed = (limit + 1) - initial_count

        if needed > 0:
            for i in range(needed):
                tbl_name = f"default.warn_tbl_{i}"
                node.query(f"CREATE TABLE {tbl_name} (a Int8) ENGINE = Log")
                tables_to_create.append(tbl_name)

        check_warning(node, warning_msg, present=True)

    finally:
        for t in tables_to_create:
            node.query(f"DROP TABLE IF EXISTS {t}")

    check_warning(node, warning_msg, present=False)


def test_named_collection_limit_warning(node):
    limit = 5
    # Metric: NamedCollection
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'NamedCollection'"))
    warning_msg = "The number of named collections"
    collections_to_create = []

    try:
        if initial_count <= limit:
            check_warning(node, warning_msg, present=False)

        needed = (limit + 1) - initial_count

        if needed > 0:
            for i in range(needed):
                col_name = f"warn_col_{i}"
                node.query(f"CREATE NAMED COLLECTION {col_name} AS key1=1")
                collections_to_create.append(col_name)

        check_warning(node, warning_msg, present=True)

    finally:
        for c in collections_to_create:
            node.query(f"DROP NAMED COLLECTION IF EXISTS {c}")

    check_warning(node, warning_msg, present=False)


def test_active_parts_limit_warning(node):
    limit = 5
    # Metric: PartsActive
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'PartsActive'"))
    warning_msg = "The number of active parts"

    table_name = "default.parts_test_table"

    try:
        if initial_count <= limit:
            check_warning(node, warning_msg, present=False)

        node.query(f"CREATE TABLE {table_name} (k Int8) ENGINE = MergeTree ORDER BY k")

        needed = (limit + 1) - initial_count

        # Insert individual rows in separate queries to create separate parts
        for i in range(needed):
            node.query(f"INSERT INTO {table_name} VALUES ({i}) SETTINGS max_block_size = 1, max_insert_block_size=1")

        # Check metric just to be sure
        current_parts = int(node.query("SELECT value FROM system.metrics WHERE metric = 'PartsActive'"))

        check_warning(node, warning_msg, present=True)

    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name}")

    # Warning might take a moment to clear as metrics update
    check_warning(node, warning_msg, present=False)

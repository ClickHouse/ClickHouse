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


def check_warning(node, message_part, present=True, retries=10):
    for _ in range(retries):
        sql = f"SELECT count() FROM system.warnings WHERE message LIKE '%{message_part}%'"
        count = int(node.query(sql).strip())

        if present and count > 0:
            return
        if not present and count == 0:
            return
        time.sleep(0.5)

    # If we reached here, the assertion failed. Get all warnings for debug.
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

    dbs_to_create = []

    try:
        check_warning(node, "The number of attached databases is more than", present=False)

        needed = (limit + 1) - initial_count

        for i in range(needed):
            db_name = f"warn_db_{i}"
            node.query(f"CREATE DATABASE {db_name}")
            dbs_to_create.append(db_name)

        check_warning(node, "The number of attached databases is more than")

    finally:
        for db in dbs_to_create:
            node.query(f"DROP DATABASE {db}")

    check_warning(node, "The number of attached databases is more than", present=False)


def test_view_limit_warning(node):
    limit = 10
    # Metric: AttachedView
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'AttachedView'"))

    views_to_create = []

    try:
        check_warning(node, "The number of attached views is more than", present=False)

        needed = (limit + 1) - initial_count
        if needed < 0: needed = 0

        for i in range(needed):
            view_name = f"default.warn_view_{i}"
            node.query(f"CREATE VIEW {view_name} AS SELECT 1")
            views_to_create.append(view_name)

        check_warning(node, "The number of attached views is more than")

    finally:
        for view in views_to_create:
            node.query(f"DROP VIEW {view}")

    check_warning(node, "The number of attached views is more than", present=False)


def test_dictionary_limit_warning(node):
    limit = 11
    # Metric: AttachedDictionary
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'AttachedDictionary'"))

    dicts_to_create = []

    try:
        check_warning(node, "The number of attached dictionaries is more than", present=False)

        needed = (limit + 1) - initial_count
        if needed < 0: needed = 0

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

        check_warning(node, "The number of attached dictionaries is more than")

    finally:
        for d in dicts_to_create:
            node.query(f"DROP DICTIONARY {d}")

    check_warning(node, "The number of attached dictionaries is more than", present=False)


def test_table_limit_warning(node):
    limit = 6
    # Metric: AttachedTable
    initial_count = int(node.query("SELECT value FROM system.metrics WHERE metric = 'AttachedTable'"))

    tables_to_create = []

    try:
        if initial_count <= limit:
            check_warning(node, "The number of attached tables is more than", present=False)

        needed = (limit + 1) - initial_count

        if needed > 0:
            for i in range(needed):
                tbl_name = f"default.warn_tbl_{i}"
                node.query(f"CREATE TABLE {tbl_name} (a Int8) ENGINE = Log")
                tables_to_create.append(tbl_name)

        check_warning(node, "The number of attached tables is more than")

    finally:
        for t in tables_to_create:
            node.query(f"DROP TABLE {t}")

    if initial_count <= limit:
        check_warning(node, "The number of attached tables is more than", present=False)

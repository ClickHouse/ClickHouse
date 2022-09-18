import time
import logging


def _parse_table_database(table, database):
    if database is not None:
        return table, database

    if "." in table:
        return reversed(table.split(".", 1))

    return table, "default"


def wait_for_delete_inactive_parts(node, table, database=None, timeout=60):
    table, database = _parse_table_database(table, database)
    inactive_parts_query = (
        f"SELECT count() FROM system.parts "
        f"WHERE not active AND table = '{table}' AND database = '{database}';"
    )
    while timeout > 0:
        if 0 == int(node.query(inactive_parts_query)):
            break
        timeout -= 1
        time.sleep(1)
    assert 0 == int(node.query(inactive_parts_query))


def wait_for_delete_empty_parts(node, table, database=None, timeout=60):
    table, database = _parse_table_database(table, database)
    empty_parts_query = (
        f"SELECT count() FROM system.parts "
        f"WHERE active AND rows = 0 AND table = '{table}' AND database = '{database}'"
    )
    while timeout > 0:
        if 0 == int(node.query(empty_parts_query)):
            break
        timeout -= 1
        time.sleep(1)
    assert 0 == int(node.query(empty_parts_query))

import time

from helpers.test_tools import assert_eq_with_retry


def _parse_table_database(table, database):
    if database is not None:
        return table, database

    if "." in table:
        return reversed(table.split(".", 1))

    return table, "default"


def wait_for_delete_inactive_parts(node, table, database=None, **kwargs):
    table, database = _parse_table_database(table, database)
    inactive_parts_query = (
        f"SELECT count() FROM system.parts "
        f"WHERE not active AND table = '{table}' AND database = '{database}';"
    )
    assert_eq_with_retry(node, inactive_parts_query, "0\n", **kwargs)


def wait_for_delete_empty_parts(node, table, database=None, **kwargs):
    table, database = _parse_table_database(table, database)
    empty_parts_query = (
        f"SELECT count() FROM system.parts "
        f"WHERE active AND rows = 0 AND table = '{table}' AND database = '{database}'"
    )
    assert_eq_with_retry(node, empty_parts_query, "0\n", **kwargs)


def wait_for_merges(node, table, database=None, **kwargs):
    table, database = _parse_table_database(table, database)
    merges_count_query = (
        f"SELECT count() > 0 FROM system.merges "
        f"WHERE table = '{table}' AND database = '{database}'"
    )
    assert_eq_with_retry(node, merges_count_query, "1\n", **kwargs)

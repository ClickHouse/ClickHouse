from typing import Any

from .test_tools import assert_eq_with_retry
from .cluster import ClickHouseInstance


def _parse_table_database(table: str, database: str | None) -> tuple[str, str]:
    if database is not None:
        return table, database

    if "." in table:
        parts = table.split(".", 1)
        return parts[1], parts[0]

    return table, "default"


def wait_for_delete_inactive_parts(node: ClickHouseInstance, table: str, database: str | None = None, **kwargs: Any) -> None:
    table, database = _parse_table_database(table, database)
    inactive_parts_query = (
        f"SELECT count() FROM system.parts "
        f"WHERE not active AND table = '{table}' AND database = '{database}';"
    )
    assert_eq_with_retry(node, inactive_parts_query, "0\n", **kwargs)


def wait_for_delete_empty_parts(node: ClickHouseInstance, table: str, database: str | None = None, **kwargs: Any) -> None:
    table, database = _parse_table_database(table, database)
    empty_parts_query = (
        f"SELECT count() FROM system.parts "
        f"WHERE active AND rows = 0 AND table = '{table}' AND database = '{database}'"
    )
    assert_eq_with_retry(node, empty_parts_query, "0\n", **kwargs)


def wait_for_merges(node: ClickHouseInstance, table: str, database: str | None = None, **kwargs: Any) -> None:
    table, database = _parse_table_database(table, database)
    merges_count_query = (
        f"SELECT count() > 0 FROM system.merges "
        f"WHERE table = '{table}' AND database = '{database}'"
    )
    assert_eq_with_retry(node, merges_count_query, "1\n", **kwargs)

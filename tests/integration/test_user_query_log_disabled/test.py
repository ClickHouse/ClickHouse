# pylint: disable=redefined-outer-name

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/query_log_disabled.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_disabled_user_query_log_name_is_not_world_readable():
    # Produce at least one query log record and materialize the table.
    node.query("SELECT 1 FORMAT Null")
    node.query("SYSTEM FLUSH LOGS")

    # With the feature disabled, `system.user_query_log` is the raw query log table (an ordinary MergeTree),
    # not the row-filtering `StorageSystemUserQueryLog`.
    engine = node.query(
        "SELECT engine FROM system.tables WHERE database = 'system' AND name = 'user_query_log'"
    ).strip()
    assert engine != ""
    assert engine != "SystemUserQueryLog"

    node.query("CREATE USER IF NOT EXISTS restricted_user IDENTIFIED WITH no_password")

    # The implicit SELECT grant on `system.user_query_log` must be gated on the feature being enabled.
    # Otherwise the raw query log, which contains every user's records, would be world-readable.
    with pytest.raises(QueryRuntimeException) as exc:
        node.query(
            "SELECT count() FROM system.user_query_log", user="restricted_user"
        )
    assert "ACCESS_DENIED" in str(exc.value)

    # A privileged user can still read it - it is a normal table now.
    assert int(node.query("SELECT count() FROM system.user_query_log")) >= 1

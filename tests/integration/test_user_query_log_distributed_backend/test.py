# pylint: disable=redefined-outer-name

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_servers.xml", "configs/query_log_distributed.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_distributed_query_log_backend_is_rejected():
    # Materialize the query log table with its configured `Distributed` engine.
    node.query("SELECT 1 FORMAT Null")
    node.query("SYSTEM FLUSH LOGS query_log")

    engine = node.query(
        "SELECT engine FROM system.tables WHERE database = 'system' AND name = 'query_log'"
    ).strip()
    assert engine == "Distributed"

    # `system.user_query_log` must refuse to read a delegating backend rather than silently forwarding
    # the read under the calling user's own identity to another server.
    with pytest.raises(QueryRuntimeException) as exc:
        node.query("SELECT count() FROM system.user_query_log")
    assert "BAD_ARGUMENTS" in str(exc.value)
    assert "delegating storage" in str(exc.value)

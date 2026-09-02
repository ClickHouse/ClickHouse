# coding: utf-8

import random
import string

import pyarrow.flight as flight
import pytest

from helpers.cluster import ClickHouseCluster

from .flight_sql_client import FlightSQLClient

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/flight_port.xml"],
)


def get_client(username=None, password=None):
    session_id = "".join(random.choices(string.ascii_letters + string.digits, k=16))
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username=username,
        password=password,
        metadata={"x-clickhouse-session-id": session_id},
        features={"metadata-reflection": "true"},
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def test_create_prepared_statement_does_not_execute_rewrite_rules():
    """Creating a prepared statement only validates syntax and infers the result schema; it must
    not apply query rewrite rules. Otherwise a REWRITE rule could turn the parsed SELECT into a
    side-effecting statement (here a DROP) and `executeQuery` would execute it during schema
    inference, even though preparing a statement must have no side effects."""
    node.query("DROP RULE rr_prepare_positive", ignore_error=True)
    node.query("DROP RULE rr_prepare_drop", ignore_error=True)
    node.query("DROP TABLE IF EXISTS default.rr_prepare_victim SYNC")

    client = get_client()

    # Positive control: a REWRITE rule active in this session DOES take effect through the normal
    # Arrow Flight execution path. This proves `query_rules` propagates to the Flight session, so
    # the prepared-statement assertion below is not vacuous.
    node.query(
        "CREATE RULE rr_prepare_positive AS (SELECT 100100100) REWRITE TO (SELECT 200200200)"
    )
    try:
        client.set_session_options({"query_rules": "rr_prepare_positive"})
        flight_info = client.execute("SELECT 100100100")
        table = client.do_get(flight_info.endpoints[0].ticket).read_all()
        assert table.column(0)[0].as_py() == 200200200
    finally:
        node.query("DROP RULE rr_prepare_positive", ignore_error=True)

    # Now the regression: a rule that rewrites a harmless SELECT into a side-effecting DROP.
    node.query("CREATE TABLE default.rr_prepare_victim (x UInt8) ENGINE = Memory")
    node.query(
        "CREATE RULE rr_prepare_drop AS (SELECT 987654321) REWRITE TO (DROP TABLE default.rr_prepare_victim)"
    )
    try:
        client.set_session_options({"query_rules": "rr_prepare_drop"})

        # Creating the prepared statement must infer the schema of the SELECT only; it must not
        # apply the rewrite rule and execute the DROP.
        stmt = client.prepare("SELECT 987654321")
        stmt.close()

        # The rule was not applied during schema inference, so the table still exists.
        assert node.query("EXISTS TABLE default.rr_prepare_victim").strip() == "1"
    finally:
        node.query("DROP RULE rr_prepare_drop", ignore_error=True)
        node.query("DROP TABLE IF EXISTS default.rr_prepare_victim SYNC")


def test_metadata_commands_do_not_apply_rewrite_rules():
    """Arrow Flight synthesizes SQL for its non-SQL requests: the metadata commands
    (`CommandGetDbSchemas`, `CommandGetTables`, ...), a `PATH` descriptor and the
    `system.settings` query behind `GetSessionOptions`. The client never submitted that SQL, so a
    rule matching it must neither reject the RPC nor rewrite its result."""
    rules = [
        # The `SELECT` behind CommandGetDbSchemas (no filter pattern).
        (
            "rr_flight_db_schemas",
            "SELECT NULL::Nullable(String) AS catalog_name, name AS db_schema_name FROM system.databases",
        ),
        # The `SELECT` behind GetSessionOptions.
        ("rr_flight_session_options", "SELECT name, value FROM system.settings"),
    ]

    for name, _ in rules:
        node.query(f"DROP RULE {name}", ignore_error=True)
    node.query("DROP RULE rr_flight_positive", ignore_error=True)
    node.query("DROP TABLE IF EXISTS default.rr_flight_tbl SYNC")
    node.query("CREATE TABLE default.rr_flight_tbl (x UInt8) ENGINE = Memory")

    for name, sql in rules:
        node.query(f"CREATE RULE {name} AS ({sql}) REJECT WITH 'blocked_{name}'")
    node.query(
        "CREATE RULE rr_flight_positive AS (SELECT 100200300) REJECT WITH 'blocked_rr_flight_positive'"
    )

    client = get_client()
    active_rules = ", ".join([name for name, _ in rules] + ["rr_flight_positive"])
    try:
        client.set_session_options({"query_rules": active_rules})

        # Positive control: the rules are active in this session and do reject the client's own SQL.
        with pytest.raises(Exception, match="blocked_rr_flight_positive"):
            client.execute("SELECT 100200300")

        # The metadata commands must succeed regardless.
        flight_info = client.get_db_schemas()
        schemas = client.do_get(flight_info.endpoints[0].ticket).read_all()
        assert "default" in schemas.column("db_schema_name").to_pylist()

        flight_info = client.get_tables(table_name_filter_pattern="rr_flight_tbl")
        tables = client.do_get(flight_info.endpoints[0].ticket).read_all()
        assert tables.column("table_name").to_pylist() == ["rr_flight_tbl"]

        # ... and so must a PATH descriptor, whose `SELECT` is synthesized from the path, ...
        descriptor = flight.FlightDescriptor.for_path("rr_flight_tbl")
        flight_info = client.client.get_flight_info(descriptor, client._flight_call_options())
        client.do_get(flight_info.endpoints[0].ticket).read_all()

        # ... and GetSessionOptions, which reads `system.settings`.
        result = client.get_session_options()
        assert "max_threads" in result.session_options
    finally:
        for name, _ in rules:
            node.query(f"DROP RULE {name}", ignore_error=True)
        node.query("DROP RULE rr_flight_positive", ignore_error=True)
        node.query("DROP TABLE IF EXISTS default.rr_flight_tbl SYNC")

# coding: utf-8

import pytest
import random
import string

from .flight_sql_client import FlightSQLClient, SetSessionOptionsResult

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
    ],
)

PROFILE_NAME = "profile_arrowflight_session_options_constraints"


def get_client():
    session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        metadata={'x-clickhouse-session-id': session_id},
        features={'metadata-reflection': 'true'},
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def settings_profile():
    node.query(f"DROP SETTINGS PROFILE IF EXISTS {PROFILE_NAME}")
    node.query(
        f"CREATE SETTINGS PROFILE {PROFILE_NAME} SETTINGS max_execution_time = 10 CONST"
    )
    try:
        yield PROFILE_NAME
    finally:
        node.query(f"DROP SETTINGS PROFILE IF EXISTS {PROFILE_NAME}")


def _query_scalar(client, query):
    flight_info = client.execute(query)
    table = client.do_get(flight_info.endpoints[0].ticket).read_all()
    return table.column(0)[0].as_py()


def test_profile_constraints_apply_within_same_request():
    """A profile set in a SetSessionOptions request constrains the other options of the same request."""
    client = get_client()

    result = client.set_session_options(
        {"profile": PROFILE_NAME, "max_execution_time": "999"}
    )

    # The profile itself is applied successfully.
    assert "profile" not in result.errors

    # The constrained setting is rejected. Constraint violations are not parse or unknown-setting
    # errors, so they map to UNSPECIFIED.
    assert "max_execution_time" in result.errors
    assert (
        result.errors["max_execution_time"].value
        == SetSessionOptionsResult.UNSPECIFIED
    )

    # The effective value stays at the one the profile sets.
    assert float(_query_scalar(client, "SELECT getSetting('max_execution_time')")) == 10

    options = client.get_session_options()
    assert float(options.session_options["max_execution_time"].string_value) == 10

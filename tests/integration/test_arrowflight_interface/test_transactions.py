import uuid

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from helpers.cluster import ClickHouseCluster

from .flight_sql_client import (
    CommandStatementUpdate,
    DoPutUpdateResult,
    FlightSQLClient,
    flight_descriptor,
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/flight_port.xml", "configs/transactions.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=[
        "filtered_list",
        "multi_read",
        "list_with_stat_and_data",
        "check_stat",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield
    finally:
        cluster.shutdown()


@pytest.fixture
def client():
    client = FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        metadata={"x-clickhouse-session-id": uuid.uuid4().hex},
    )
    node.query(
        "CREATE TABLE flight_transactions "
        "(id UInt64, CONSTRAINT positive CHECK id > 0) "
        "ENGINE = MergeTree ORDER BY id"
    )
    try:
        yield client
    finally:
        client.client.close()
        node.query("DROP TABLE flight_transactions SYNC")


def test_doput_commits_before_response(client):
    for iteration in range(100):
        command = CommandStatementUpdate(
            query=f"INSERT INTO flight_transactions SELECT {iteration + 1} "
            "SETTINGS implicit_transaction = 1, async_insert = 0, "
            "wait_changes_become_visible_after_commit_mode = 'wait'"
        )
        writer, reader = client.client.do_put(
            flight_descriptor(command), pa.schema([]), client._flight_call_options()
        )
        try:
            metadata = reader.read()
            assert metadata is not None

            # Start the next transaction as soon as metadata arrives, before closing the original writer.
            client.execute_update("BEGIN TRANSACTION")
            try:
                info = client.execute("SELECT count() FROM flight_transactions")
                table = client.do_get(info.endpoints[0].ticket).read_all()
                assert table.column(0).to_pylist() == [iteration + 1]
            finally:
                client.execute_update("ROLLBACK")

            result = DoPutUpdateResult()
            result.ParseFromString(metadata.to_pybytes())
            assert result.record_count == 1
        finally:
            writer.close()

    assert node.query("SELECT count(), sum(id) FROM flight_transactions") == "100\t5050\n"


def test_failed_doput_releases_transaction_and_session(client):
    with pytest.raises(flight.FlightServerError, match="Constraint.*positive"):
        client.execute_update(
            "INSERT INTO flight_transactions SELECT 0 "
            "SETTINGS implicit_transaction = 1, async_insert = 0"
        )

    client.execute_update("BEGIN TRANSACTION")
    try:
        info = client.execute("SELECT count() FROM flight_transactions")
        table = client.do_get(info.endpoints[0].ticket).read_all()
        assert table.column(0).to_pylist() == [0]
    finally:
        client.execute_update("ROLLBACK")

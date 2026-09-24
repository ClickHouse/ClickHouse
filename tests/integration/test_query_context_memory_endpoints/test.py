import contextlib
import io
import struct
import time
import uuid

import grpc
import psycopg
import psycopg2
import pymysql
import pytest
import requests
import snappy
from pyarrow import flight

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from test_arrowflight_interface.flight_sql_client import (
    CancelStatus,
    CommandStatementQuery,
    FlightSQLClient,
    flight_descriptor,
)
from test_default_session_user.grpc_protocol_pb2 import clickhouse_grpc_pb2 as grpc_pb2
from test_prometheus_protocols.prometheus_test_utils import (
    convert_time_series_to_protobuf,
    convert_read_request_to_protobuf,
    extract_protobuf_from_remote_read_response,
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/protocols.xml"])
PORTS = {
    "mysql": 9004,
    "postgres": 9005,
    "grpc": 9100,
    "flight": 8888,
    "prometheus": 9093,
}
PAYLOAD_SIZE = 8 * 1024 * 1024
SELECT = "SELECT 1 SETTINGS log_comment = ''"
ENDPOINTS = [
    "native",
    "http",
    "mysql",
    "mysql_prepared",
    "postgres_simple",
    "postgres_extended",
    "postgres_execute",
    "postgres_copy_from",
    "postgres_copy_to",
    "grpc_unary_unary",
    "grpc_stream_unary",
    "grpc_unary_stream",
    "grpc_stream_stream",
    "flight_info",
    "flight_schema",
    "flight_get",
    "flight_put",
    "flight_poll",
    "flight_cancel",
    "flight_prepare",
    "flight_prepare_insert",
    "flight_session_options",
    "flight_metadata",
    "prometheus_query",
    "prometheus_query_range",
    "prometheus_format_query",
    "prometheus_series",
    "prometheus_labels",
    "prometheus_label_values",
    "prometheus_metadata",
    "prometheus_read",
    "prometheus_write",
    "prometheus_write_empty",
]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        if node.is_built_with_sanitizer():
            pytest.skip(
                "Requires ClickHouse allocation interceptors, which sanitizer builds replace"
            )
        node.query("CREATE TABLE context_memory_sink (n UInt64) ENGINE = Null")
        node.query(
            "CREATE TABLE context_memory_prometheus ENGINE = TimeSeries",
            settings={"allow_experimental_time_series_table": 1},
        )
        node.query(
            "CREATE SETTINGS PROFILE context_memory_payload SETTINGS "
            "log_queries = 0, enable_time_series_aggregate_functions = 1, log_comment = '"
            + "x" * PAYLOAD_SIZE
            + "'",
            settings={"max_query_size": 2 * PAYLOAD_SIZE, "log_queries": 0},
        )
        node.query(
            "CREATE SETTINGS PROFILE context_memory_control SETTINGS "
            "log_queries = 0, enable_time_series_aggregate_functions = 1, log_comment = ''"
        )
        yield
    finally:
        cluster.shutdown()


@contextlib.contextmanager
def payload_user(limit, batching_limit, profile="context_memory_payload"):
    user = "context_memory_" + uuid.uuid4().hex
    node.query(
        f"CREATE USER {user} SETTINGS PROFILE {profile}, "
        f"{limit} = {PAYLOAD_SIZE // 2}, max_untracked_memory = {batching_limit}"
    )
    node.query(f"GRANT SELECT, INSERT, CREATE TEMPORARY TABLE ON *.* TO {user}")
    try:
        yield user
    finally:
        node.query(f"DROP USER {user}")


# Every adapter consumes the response, so errors in streamed execution are observed too.
def run_endpoint(endpoint, user):
    if endpoint == "native":
        assert node.query(SELECT, user=user) == "1\n"
    elif endpoint == "http":
        assert node.http_query(SELECT, user=user) == "1\n"
    elif endpoint in ("mysql", "mysql_prepared"):
        with pymysql.connect(
            host=node.ip_address,
            port=PORTS["mysql"],
            user=user,
            database="default",
            read_timeout=30,
            write_timeout=30,
        ) as connection:
            if endpoint == "mysql_prepared":
                connection._execute_command(
                    0x16,
                    "INSERT INTO context_memory_sink SETTINGS log_comment = '' VALUES (1)",
                )
                packet = connection._read_packet()
                packet.read_uint8()
                statement_id = packet.read_uint32()
                try:
                    connection._execute_command(
                        0x17, struct.pack("<IBI", statement_id, 0, 1)
                    )
                    assert connection._read_packet().is_ok_packet()
                finally:
                    connection._execute_command(0x19, struct.pack("<I", statement_id))
            else:
                with connection.cursor() as cursor:
                    cursor.execute(SELECT)
                    assert cursor.fetchone() == (1,)
    elif endpoint.startswith("postgres_"):
        if endpoint == "postgres_extended":
            with psycopg.connect(
                host=node.ip_address,
                port=PORTS["postgres"],
                user=user,
                dbname="default",
                autocommit=True,
                connect_timeout=10,
            ) as connection:
                assert connection.execute(SELECT, prepare=True).fetchone() == (1,)
        else:
            with contextlib.closing(
                psycopg2.connect(
                    host=node.ip_address,
                    port=PORTS["postgres"],
                    user=user,
                    dbname="default",
                    connect_timeout=10,
                )
            ) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    if endpoint == "postgres_copy_from":
                        cursor.copy_expert(
                            "COPY context_memory_sink FROM STDIN", io.StringIO("1\n")
                        )
                    elif endpoint == "postgres_copy_to":
                        cursor.copy_expert(
                            "COPY context_memory_sink TO STDOUT", io.StringIO()
                        )
                    else:
                        if endpoint == "postgres_execute":
                            cursor.execute(
                                "PREPARE context_memory_statement AS " + SELECT
                            )
                            cursor.execute("EXECUTE context_memory_statement")
                        else:
                            cursor.execute(SELECT)
                        assert cursor.fetchone() == (1,)
    elif endpoint.startswith("grpc_"):
        kind = endpoint.removeprefix("grpc_")
        method = {
            "unary_unary": "ExecuteQuery",
            "stream_unary": "ExecuteQueryWithStreamInput",
            "unary_stream": "ExecuteQueryWithStreamOutput",
            "stream_stream": "ExecuteQueryWithStreamIO",
        }[kind]
        with grpc.insecure_channel(f"{node.ip_address}:{PORTS['grpc']}") as channel:
            call = getattr(channel, kind)(
                "/clickhouse.grpc.ClickHouse/" + method,
                request_serializer=grpc_pb2.QueryInfo.SerializeToString,
                response_deserializer=grpc_pb2.Result.FromString,
            )
            query = grpc_pb2.QueryInfo(
                query=SELECT, user_name=user, output_format="TabSeparated"
            )
            result = call(
                iter([query]) if kind.startswith("stream") else query, timeout=30
            )
            results = list(result) if kind.endswith("stream") else [result]
            for result in results:
                if result.HasField("exception"):
                    raise RuntimeError(result.exception.display_text)
            assert b"".join(result.output for result in results) == b"1\n"
    elif endpoint.startswith("flight_"):
        client = FlightSQLClient(
            node.ip_address,
            PORTS["flight"],
            insecure=True,
            username=user,
            metadata=(
                {"x-clickhouse-session-id": str(uuid.uuid4())}
                if endpoint == "flight_session_options"
                else None
            ),
        )
        try:
            client._flight_call_options = lambda: flight.FlightCallOptions(
                headers=client.headers,
                timeout=30,
            )
            if endpoint == "flight_schema":
                assert len(client.get_schema(SELECT).schema) == 1
            elif endpoint == "flight_get":
                assert client.do_get(flight.Ticket(SELECT)).read_all().num_rows == 1
            elif endpoint == "flight_put":
                client.execute_update(
                    "INSERT INTO context_memory_sink SETTINGS log_comment = '' VALUES (1)"
                )
            elif endpoint in ("flight_prepare", "flight_prepare_insert"):
                statement = client.prepare(
                    SELECT
                    if endpoint == "flight_prepare"
                    else "INSERT INTO context_memory_sink VALUES (?)"
                )
                try:
                    if endpoint == "flight_prepare":
                        assert statement.dataset_schema is not None
                finally:
                    statement.close()
            elif endpoint == "flight_session_options":
                assert not client.set_session_options({"max_block_size": 123}).errors
            elif endpoint in ("flight_poll", "flight_cancel"):
                poll = client.poll_flight_info(
                    flight_descriptor(CommandStatementQuery(query=SELECT))
                )
                if endpoint == "flight_cancel":
                    result = client.cancel_flight_info(poll.info_bytes)
                    assert result.status == CancelStatus.Value(
                        "CANCEL_STATUS_CANCELLED"
                    )
                    return
                deadline = time.monotonic() + 30
                while poll.flight_descriptor is not None:
                    assert time.monotonic() < deadline, "Flight polling did not finish"
                    poll = client.poll_flight_info(poll.flight_descriptor)
                assert (
                    sum(
                        client.do_get(e.ticket).read_all().num_rows
                        for e in poll.info.endpoints
                    )
                    == 1
                )
            else:
                info = (
                    client.get_db_schemas()
                    if endpoint == "flight_metadata"
                    else client.execute(SELECT)
                )
                rows = sum(
                    client.do_get(e.ticket).read_all().num_rows for e in info.endpoints
                )
                assert rows >= 1
        finally:
            client.client.close()
    elif endpoint.startswith("prometheus_"):
        kind = endpoint.removeprefix("prometheus_")
        url = f"http://{node.ip_address}:{PORTS['prometheus']}"
        kwargs = {"auth": (user, ""), "timeout": 30, "params": {"log_comment": ""}}
        if kind in ("read", "write", "write_empty"):
            if kind.startswith("write"):
                message = convert_time_series_to_protobuf(
                    [({"__name__": "context_memory"}, {1: 1})]
                    if kind == "write"
                    else []
                )
            else:
                message = convert_read_request_to_protobuf("context_memory", 0, 2)
            response = requests.post(
                url + "/" + ("write" if kind.startswith("write") else "read"),
                data=snappy.compress(message.SerializeToString()),
                headers={
                    "Content-Encoding": "snappy",
                    "Content-Type": "application/x-protobuf",
                },
                **kwargs,
            )
        else:
            path = "label/__name__/values" if kind == "label_values" else kind
            if kind in ("query", "query_range", "format_query"):
                kwargs["params"]["query"] = "1"
            if kind == "query_range":
                kwargs["params"].update(start=0, end=2, step=1)
            elif kind in ("series", "labels", "label_values"):
                kwargs["params"].update(
                    {"match[]": "context_memory", "start": 0, "end": 2}
                )
            response = requests.post(url + "/api/v1/" + path, **kwargs)
        if response.status_code not in (200, 204):
            raise RuntimeError(response.text)
        if kind == "read":
            assert (
                len(extract_protobuf_from_remote_read_response(response).results) == 1
            )
        elif not kind.startswith("write"):
            assert response.json()["status"] == "success", response.text
    else:
        raise ValueError(endpoint)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
@pytest.mark.parametrize(
    "limit,level",
    [("max_memory_usage", "Query"), ("max_memory_usage_for_user", "User")],
)
def test_endpoint_setup_limit_and_recovery(endpoint, batching_limit, limit, level):
    # The large setting is inherited from the session before the query clears it.
    # The logical value is small, but its copied capacity still belongs to this query.
    with payload_user(limit, batching_limit) as user:
        with pytest.raises(
            Exception, match=f"{level} memory limit exceeded during query setup"
        ):
            run_endpoint(endpoint, user)
        node.query(f"ALTER USER {user} MODIFY SETTINGS {limit} = 0")
        run_endpoint(endpoint, user)


@pytest.mark.parametrize(
    "endpoint",
    [
        "prometheus_query",
        "prometheus_format_query",
        "prometheus_read",
        "prometheus_write_empty",
    ],
)
@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
@pytest.mark.parametrize(
    "limit,level",
    [("max_memory_usage", "Query"), ("max_memory_usage_for_user", "User")],
)
def test_prometheus_response_buffer_limit_and_recovery(
    endpoint, batching_limit, limit, level
):
    with payload_user(limit, batching_limit, "context_memory_control") as user:
        node.query(
            f"ALTER USER {user} MODIFY SETTINGS http_response_buffer_size = {PAYLOAD_SIZE}"
        )
        with pytest.raises(Exception, match=f"{level} memory limit exceeded"):
            run_endpoint(endpoint, user)
        node.query(f"ALTER USER {user} MODIFY SETTINGS {limit} = 0")
        run_endpoint(endpoint, user)


@pytest.mark.parametrize(
    "route,params,status",
    [
        ("parse_query", {"query": "1"}, 400),
        ("unknown_endpoint", {}, 404),
        ("query", {"query": ")"}, 400),
        ("metadata", {"limit": "invalid"}, 400),
    ],
)
@pytest.mark.parametrize("allocation", ["context", "response"])
@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
@pytest.mark.parametrize(
    "limit,level",
    [("max_memory_usage", "Query"), ("max_memory_usage_for_user", "User")],
)
def test_prometheus_error_response_limit_and_recovery(
    route, params, status, allocation, batching_limit, limit, level
):
    profile = (
        "context_memory_payload" if allocation == "context" else "context_memory_control"
    )
    buffer_size = PAYLOAD_SIZE if allocation == "response" else 65536
    with payload_user(limit, batching_limit, profile) as user:
        def request():
            return requests.post(
                f"http://{node.ip_address}:{PORTS['prometheus']}/api/v1/{route}",
                auth=(user, ""),
                params={
                    **params,
                    "log_comment": "",
                    "http_response_buffer_size": buffer_size,
                },
                timeout=30,
            )

        response = request()
        assert response.status_code >= 400
        assert f"{level} memory limit exceeded" in response.text
        node.query(f"ALTER USER {user} MODIFY SETTINGS {limit} = 0")
        response = request()
        assert response.status_code == status, response.text
        assert response.json()["status"] == "error", response.text


def endpoint_cleanup_balance(endpoint, batching_limit, profile, expected_balance=None):
    with payload_user("max_memory_usage", batching_limit, profile) as user:
        # Keep retained `query_metric_log` bookkeeping out of the context balance.
        node.query(
            f"ALTER USER {user} MODIFY SETTINGS max_memory_usage = 0, query_metric_log_interval = 0"
        )
        sentinel_id = str(uuid.uuid4())
        sentinel = node.get_query_request(
            "SELECT repeat('ssssssssssssssssssssssssssssssss', 524288), sleep(600) "
            "SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 10000000000 "
            "FORMAT Null",
            user=user,
            query_id=sentinel_id,
            settings={"max_untracked_memory": 0, "log_queries": 0},
        )
        try:
            assert_eq_with_retry(
                node,
                f"SELECT count() FROM system.processes WHERE query_id = '{sentinel_id}'",
                "1",
            )
            for _ in range(2):
                run_endpoint(endpoint, user)
            balance_query = (
                "SELECT memory_usage - (SELECT memory_usage FROM system.processes "
                f"WHERE query_id = '{sentinel_id}') FROM system.user_processes WHERE user = '{user}'"
            )
            # Protocol completion can precede context destruction on the server thread.
            if expected_balance is not None:
                assert_eq_with_retry(
                    node,
                    f"SELECT abs(({balance_query}) - ({expected_balance[0]})) < 65536",
                    "1",
                )
            before = int(node.query(balance_query))
            for _ in range(8):
                run_endpoint(endpoint, user)
            # Keep a live query and a positive balance: neither a last-query reset nor
            # saturation at zero may hide a context freed under the wrong tracker.
            assert (
                int(
                    node.query(
                        f"SELECT memory_usage FROM system.processes WHERE query_id = '{sentinel_id}'"
                    )
                )
                >= 16 * 1024 * 1024
            )
            delta_query = f"SELECT ({balance_query}) - ({before})"
            if expected_balance is not None:
                assert_eq_with_retry(
                    node,
                    f"SELECT abs(({delta_query}) - ({expected_balance[1]})) < 65536",
                    "1",
                )
            return before, int(node.query(delta_query))
        finally:
            node.query(f"KILL QUERY WHERE query_id = '{sentinel_id}' SYNC")
            sentinel.get_answer_and_error()


@pytest.mark.parametrize("endpoint", ENDPOINTS)
@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
def test_endpoint_releases_context_memory(endpoint, batching_limit):
    # Initialize shared protocol state before measuring the copied context.
    with payload_user(
        "max_memory_usage", batching_limit, "context_memory_control"
    ) as user:
        node.query(f"ALTER USER {user} MODIFY SETTINGS max_memory_usage = 0")
        run_endpoint(endpoint, user)
    expected_balance = (0, 0)
    if endpoint in ("mysql_prepared", "flight_put", "prometheus_write"):
        # Subtract insert execution drift to isolate the copied setup context.
        expected_balance = endpoint_cleanup_balance(
            endpoint, batching_limit, "context_memory_control"
        )
    endpoint_cleanup_balance(
        endpoint, batching_limit, "context_memory_payload", expected_balance
    )


@pytest.mark.parametrize("batching_limit", [0, 4 * 1024 * 1024])
@pytest.mark.parametrize("query", ["SELECT 1", "SELECT throwIf(1)"])
def test_grpc_last_query_cleanup(query, batching_limit):
    failpoint = "grpc_pause_after_query_id_release"
    with payload_user("max_memory_usage", batching_limit) as user:
        node.query(
            f"ALTER USER {user} MODIFY SETTINGS max_memory_usage = 0, query_metric_log_interval = 0"
        )
        with grpc.insecure_channel(f"{node.ip_address}:{PORTS['grpc']}") as channel:
            call = channel.unary_unary(
                "/clickhouse.grpc.ClickHouse/ExecuteQuery",
                request_serializer=grpc_pb2.QueryInfo.SerializeToString,
                response_deserializer=grpc_pb2.Result.FromString,
            )
            node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
            sentinel = None
            sentinel_id = str(uuid.uuid4())
            try:
                result = call.future(
                    grpc_pb2.QueryInfo(query=query, user_name=user), timeout=60
                )
                node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=15)
                assert (
                    node.query(
                        f"SELECT count() FROM system.processes WHERE user = '{user}'"
                    )
                    == "0\n"
                )
                sentinel = node.get_query_request(
                    "SELECT repeat('ssssssssssssssssssssssssssssssss', 524288), sleep(600) "
                    "SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 10000000000 "
                    "FORMAT Null",
                    user=user,
                    query_id=sentinel_id,
                    settings={"max_untracked_memory": 0, "log_queries": 0},
                )
                assert_eq_with_retry(
                    node,
                    f"SELECT count() FROM system.processes WHERE query_id = '{sentinel_id}'",
                    "1",
                )
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
                response = result.result(timeout=30)
                assert response.HasField("exception") == ("throwIf" in query)
                for _ in range(20):
                    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
                    if (
                        node.query(
                            "SELECT toUInt64(value) FROM system.asynchronous_metrics WHERE metric = 'GRPCThreads'"
                        )
                        == "0\n"
                    ):
                        break
                    time.sleep(0.1)
                else:
                    pytest.fail("gRPC call did not finish cleanup")
                balance = int(
                    node.query(
                        "SELECT memory_usage - (SELECT memory_usage FROM system.processes "
                        f"WHERE query_id = '{sentinel_id}') FROM system.user_processes WHERE user = '{user}'"
                    )
                )
                assert abs(balance) < 65536, balance
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
                if sentinel is not None:
                    node.query(f"KILL QUERY WHERE query_id = '{sentinel_id}' SYNC")
                    sentinel.get_answer_and_error()


def test_postgres_multistatement_contexts():
    with payload_user("max_memory_usage", 0) as user:
        node.query(f"ALTER USER {user} MODIFY SETTINGS max_memory_usage = 0")
        with contextlib.closing(
            psycopg2.connect(
                host=node.ip_address,
                port=PORTS["postgres"],
                user=user,
                dbname="default",
                connect_timeout=10,
            )
        ) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    "SET max_block_size = 123; SELECT getSetting('max_block_size')"
                )
                assert cursor.fetchone() == (123,)
                cursor.execute("SELECT 1; SELECT 2")
                assert cursor.fetchone() == (2,)


def test_flight_session_options_preserve_session_memory():
    client = FlightSQLClient(
        node.ip_address,
        PORTS["flight"],
        insecure=True,
        username="default",
        metadata={"x-clickhouse-session-id": str(uuid.uuid4())},
    )
    try:
        assert not client.set_session_options(
            {"log_comment": "x" * (256 * 1024)}
        ).errors
        assert not client.set_session_options({"max_block_size": 123}).errors
        options = client.get_session_options().session_options
        assert options["max_block_size"].string_value == "123"
        assert len(options["log_comment"].string_value) == 256 * 1024
        assert not client.set_session_options(
            {"log_comment": None, "max_block_size": None}
        ).errors
    finally:
        client.client.close()

# coding: utf-8
# Regression test: a malformed ArrowFlight prepared-statement parameter batch (a one-row String
# whose offsets buffer is too small for the declared length) must be rejected with a clean error,
# not read out of bounds by arrow::Array::GetScalar in buildQueryWithValues.
#
# The malformed batch cannot be produced by pyarrow's high-level writer (it validates buffers on
# serialization), so the parameters are bound via a raw gRPC DoPut with hand-patched Arrow IPC
# bytes, using the same session as the high-level client that prepares and executes the statement.

import random
import string
import struct

import grpc
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from .flight_sql_client import FlightSQLClient

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/flight_port.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def _make_client(session_id):
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        metadata={"x-clickhouse-session-id": session_id},
        features={"metadata-reflection": "true"},
    )


# --- minimal protobuf wire encoding (avoids depending on generated Flight stubs) ---

def _varint(value):
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _bytes_field(field_no, data):
    return _varint((field_no << 3) | 2) + _varint(len(data)) + data


def _varint_field(field_no, value):
    return _varint((field_no << 3) | 0) + _varint(value)


_COMMAND_TYPE_URL = (
    "type.googleapis.com/arrow.flight.protocol.sql.CommandPreparedStatementQuery"
)


def _prepared_query_descriptor(handle):
    """FlightDescriptor{ type = CMD, cmd = Any(CommandPreparedStatementQuery{handle}) }."""
    command = _bytes_field(1, handle)  # CommandPreparedStatementQuery.prepared_statement_handle
    any_msg = _bytes_field(1, _COMMAND_TYPE_URL.encode()) + _bytes_field(2, command)
    return _varint_field(1, 2) + _bytes_field(2, any_msg)  # FlightDescriptor.type = CMD(2), cmd


def _flight_data(descriptor=b"", data_header=b"", data_body=b""):
    """FlightData{ flight_descriptor=1, data_header=2, data_body=1000 }."""
    out = b""
    if descriptor:
        out += _bytes_field(1, descriptor)
    if data_header:
        out += _bytes_field(2, data_header)
    if data_body:
        out += _bytes_field(1000, data_body)
    return out


def _malformed_string_param_messages():
    """A one-row String batch whose offsets buffer is shrunk from 8 to 4 bytes (one int32 offset
    only) and whose data buffer is emptied, so the declared length of 1 has no valid offsets.
    Returns the schema IPC message metadata, the patched record-batch metadata, and the body."""
    batch = pa.record_batch([pa.array(["x"], type=pa.string())], names=["param_1"])
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    reader = ipc.MessageReader.open_stream(sink.getvalue())
    messages = []
    while True:
        try:
            msg = reader.read_next_message()
        except StopIteration:
            break
        if msg is None:
            break
        messages.append(msg)
    assert len(messages) == 2, f"expected schema + record batch, got {len(messages)}"
    schema_msg, batch_msg = messages

    metadata = bytearray(batch_msg.metadata)
    body = bytes(batch_msg.body)[:4]
    # Buffer metadata for a one-row String batch: buffers[1] = offsets {offset, length=8},
    # buffers[2] = data {offset=8, length=1}. Shrink offsets to 4 bytes and empty the data buffer.
    offsets_len_off, data_offset_off, data_len_off = 104, 112, 120
    assert struct.unpack_from("<q", metadata, offsets_len_off)[0] == 8, "unexpected IPC layout"
    struct.pack_into("<q", metadata, offsets_len_off, 4)
    struct.pack_into("<q", metadata, data_offset_off, 4)
    struct.pack_into("<q", metadata, data_len_off, 0)
    return bytes(schema_msg.metadata), bytes(metadata), body


def _raw_bind_parameters(session_id, handle, schema_header, batch_header, batch_body):
    descriptor = _prepared_query_descriptor(handle)
    requests = iter(
        [
            _flight_data(descriptor=descriptor, data_header=schema_header),
            _flight_data(data_header=batch_header, data_body=batch_body),
        ]
    )
    channel = grpc.insecure_channel(f"{node.ip_address}:8888")
    try:
        do_put = channel.stream_stream(
            "/arrow.flight.protocol.FlightService/DoPut",
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )
        list(do_put(requests, metadata=[("x-clickhouse-session-id", session_id)], timeout=30))
    finally:
        channel.close()


def test_prepared_statement_malformed_string_param_is_rejected():
    session_id = "".join(random.choices(string.ascii_letters + string.digits, k=16))
    client = _make_client(session_id)

    stmt = client.prepare("SELECT ?")
    schema_header, batch_header, batch_body = _malformed_string_param_messages()

    # Bind the malformed parameter batch out of band, in the same session.
    _raw_bind_parameters(session_id, stmt.handle, schema_header, batch_header, batch_body)

    # Executing must surface the Arrow buffer-validation error (params->ValidateFull rejects the
    # malformed offsets before GetScalar reads them), not crash the server. Assert it is that
    # specific error and not an unrelated binding/session/handle failure that would also raise.
    with pytest.raises(Exception) as exc_info:
        stmt.execute()
    message = str(exc_info.value)
    assert ("Offsets buffer" in message) or ("isn't large enough" in message), (
        f"expected an Arrow offsets-buffer validation error, got: {message}"
    )
    lowered = message.lower()
    for unrelated in ("not bound", "session", "prepared statement handle", "does not exist"):
        assert unrelated not in lowered, f"rejection came from an unrelated error ({unrelated!r}): {message}"

    stmt.close()

    # The server must still be responsive after rejecting the malformed parameters.
    alive = _make_client("".join(random.choices(string.ascii_letters + string.digits, k=16)))
    flight_info = alive.execute("SELECT 1 AS x")
    table = alive.do_get(flight_info.endpoints[0].ticket).read_all()
    assert table.column("x")[0].as_py() == 1

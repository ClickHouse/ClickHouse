import socket
import struct
import time

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")

OLD_CLIENT_REVISION = 54452
# Keep exact send-count assertions independent of sanitizer load. This is longer
# than the test timeout, so time-based interactive updates cannot add sends.
INTERACTIVE_DELAY_ONE_HOUR = 3_600_000_000

# An empty `Native` block (`BlockInfo` terminator, zero columns, zero rows) in a
# ClickHouse compressed frame using the `LZ4` codec. The checksum covers the
# frame header and payload after the first 16 bytes.
COMPRESSED_EMPTY_BLOCK = bytes.fromhex(
    "a8cc738db073ad6baee9b44ce365092c" "820d0000000300000030000000"
)


def encode_varuint(value):
    result = bytearray()
    while value > 0x7F:
        result.append(0x80 | (value & 0x7F))
        value >>= 7
    result.append(value)
    return bytes(result)


def encode_string(value):
    if isinstance(value, str):
        value = value.encode()
    return encode_varuint(len(value)) + value


def decode_varuint(sock):
    result = 0
    shift = 0
    while True:
        byte = sock.recv(1)
        if not byte:
            raise ConnectionError("Connection closed while reading VarUInt")
        result |= (byte[0] & 0x7F) << shift
        if byte[0] < 0x80:
            return result
        shift += 7


def skip_string(sock):
    remaining = decode_varuint(sock)
    while remaining:
        data = sock.recv(remaining)
        if not data:
            raise ConnectionError("Connection closed while reading String")
        remaining -= len(data)


def send_old_revision_query(
    query,
    settings=None,
    receive_end_of_stream_before_data=False,
    compression=False,
):
    sock = socket.create_connection((node.ip_address, 9000), timeout=10)
    try:
        hello = bytearray()
        hello += encode_varuint(0)  # `Client::Hello`
        hello += encode_string("ClickHouse integration test")
        hello += encode_varuint(22)
        hello += encode_varuint(3)
        hello += encode_varuint(OLD_CLIENT_REVISION)
        hello += encode_string("")  # default database
        hello += encode_string("default")
        hello += encode_string("")  # password
        sock.sendall(hello)

        assert decode_varuint(sock) == 0  # `Server::Hello`
        skip_string(sock)  # server name
        decode_varuint(sock)  # version major
        decode_varuint(sock)  # version minor
        decode_varuint(sock)  # protocol revision
        skip_string(sock)  # timezone
        skip_string(sock)  # display name
        decode_varuint(sock)  # version patch

        packet = bytearray()
        packet += encode_varuint(1)  # `Client::Query`
        packet += encode_string("old-revision-grouped-writes")
        packet += struct.pack("<B", 1)  # `ClientInfo::INITIAL_QUERY`
        packet += encode_string("default")  # initial user
        packet += encode_string("")  # initial query id
        packet += encode_string("127.0.0.1:9000")  # initial address
        packet += struct.pack("<Q", int(time.time() * 1_000_000))
        packet += struct.pack("<B", 1)  # `ClientInfo::TCP`
        packet += encode_string("integration-test")  # OS user
        packet += encode_string("integration-test")  # client hostname
        packet += encode_string("ClickHouse integration test")
        packet += encode_varuint(22)
        packet += encode_varuint(3)
        packet += encode_varuint(OLD_CLIENT_REVISION)
        packet += encode_string("")  # quota key
        packet += encode_varuint(0)  # distributed depth
        packet += encode_varuint(1)  # version patch
        packet += struct.pack("<B", 0)  # no `OpenTelemetry` trace context
        if settings is None:
            settings = (
                ("send_logs_level", "trace"),
                ("send_profile_events", "1"),
                ("interactive_delay", str(INTERACTIVE_DELAY_ONE_HOUR)),
            )
        for name, value in settings:
            packet += encode_string(name)
            packet += encode_varuint(1)  # `BaseSettingsHelpers::Flags::IMPORTANT`
            packet += encode_string(value)
        packet += encode_string("")  # settings terminator
        packet += encode_string("")  # interserver secret hash
        packet += encode_varuint(2)  # `QueryProcessingStage::Complete`
        packet += encode_varuint(int(compression))
        packet += encode_string(query)
        sock.sendall(packet)

        if receive_end_of_stream_before_data:
            # This is deliberately a protocol-order assertion, not a timing
            # assertion: a detached query must respond before requiring the
            # mandatory trailing empty block from the client.
            assert decode_varuint(sock) == 5  # `Server::EndOfStream`

        empty_block = bytearray()
        empty_block += encode_varuint(2)  # `Client::Data`
        empty_block += encode_string("")  # temporary table name
        if compression:
            empty_block += COMPRESSED_EMPTY_BLOCK
        else:
            empty_block += encode_varuint(0)  # BlockInfo end marker
            empty_block += encode_varuint(0)  # columns
            empty_block += encode_varuint(0)  # rows
        sock.sendall(empty_block)
        sock.shutdown(socket.SHUT_WR)

        while sock.recv(64 * 1024):
            pass
    finally:
        sock.close()


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def native_send_count():
    # Sampling over HTTP does not use `TCPHandlerPocoChunkedWriter`, so the two
    # observations do not change the counter being tested.
    return int(
        node.http_query(
            "SELECT sum(value) FROM system.events "
            "WHERE event = 'NativeProtocolSend'"
        )
    )


@pytest.mark.parametrize("compression", [0, 1])
def test_select_groups_terminal_packets(compression):
    sends_before = native_send_count()
    answer, logs = node.query_and_get_answer_with_error(
        "SELECT 1",
        settings={
            "compression": compression,
            "interactive_delay": INTERACTIVE_DELAY_ONE_HOUR,
            "send_logs_level": "trace",
            "send_profile_events": 1,
        },
    )
    assert answer == "1\n"
    assert "Trace" in logs

    # Server hello, logs before reading temporary tables, header block, data block,
    # and one grouped terminal write. The terminal write contains profile
    # information, progress, the empty data block, final progress, logs, profile
    # events, and `EndOfStream`.
    assert native_send_count() - sends_before == 5


def test_old_revision_groups_uncompressed_terminal_packets():
    node.http_query("DROP TABLE IF EXISTS old_revision_probe")
    sends_before = native_send_count()
    send_old_revision_query(
        "CREATE TABLE old_revision_probe (x UInt8) ENGINE = Memory"
    )

    assert node.http_query("EXISTS TABLE old_revision_probe") == "1\n"
    # Server hello, logs flushed before reading temporary tables, and one grouped
    # query response. Revision 54452 predates compressed Log/ProfileEvents columns,
    # so both writers use raw `out` even when `maybe_compressed_out` has not been
    # initialized yet.
    assert native_send_count() - sends_before == 3


def test_old_revision_groups_compressed_terminal_packets():
    node.http_query("DROP TABLE IF EXISTS old_revision_compressed_probe")
    sends_before = native_send_count()
    send_old_revision_query(
        "CREATE TABLE old_revision_compressed_probe (x UInt8) ENGINE = Memory",
        compression=True,
    )

    assert node.http_query("EXISTS TABLE old_revision_compressed_probe") == "1\n"
    # Regular `Data` uses the compressed wrapper, but revision 54452 still writes
    # `Log` and `ProfileEvents` blocks to raw `out`. They must remain in the grouped
    # terminal write instead of flushing raw `out` separately.
    assert native_send_count() - sends_before == 3


def test_background_query_responds_before_trailing_data():
    send_old_revision_query(
        "SELECT 1",
        settings=(("run_query_in_background", "1"),),
        receive_end_of_stream_before_data=True,
    )


def test_nonempty_data_does_not_wait_for_interactive_delay():
    sends_before = native_send_count()
    assert (
        node.query(
            "SELECT number, sleepEachRow(0.25) FROM numbers(2)",
            settings={
                "compression": 1,
                "max_block_size": 1,
                "interactive_delay": INTERACTIVE_DELAY_ONE_HOUR,
            },
        )
        == "0\t0\n1\t0\n"
    )

    # Server hello, header block, two separately flushed data blocks, and the
    # grouped terminal write. Without the per-iteration `sync`, both rows remain
    # buffered until the terminal write because `interactive_delay` is one hour.
    assert native_send_count() - sends_before == 5


@pytest.mark.parametrize("async_insert", [0, 1])
def test_insert_schema_and_completion_are_flushed(async_insert):
    table_name = f"insert_target_{async_insert}"
    node.query(f"CREATE TABLE {table_name} (x UInt64) ENGINE = Memory")
    sends_before = native_send_count()

    node.query(
        f"INSERT INTO {table_name} FORMAT TSV",
        stdin="1\n2\n",
        settings={
            "async_insert": async_insert,
            "compression": 1,
            "send_profile_events": 1,
            "wait_for_async_insert": 1,
        },
        timeout=10,
    )

    # The schema must be visible before the client can encode its data. Completion
    # packets must then be visible before the client can finish the query.
    assert native_send_count() - sends_before == 4
    assert node.query(f"SELECT groupArray(x) FROM {table_name}") == "[1,2]\n"

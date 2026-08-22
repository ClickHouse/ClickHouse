import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


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
    assert (
        node.query("SELECT 1", settings={"compression": compression})
        == "1\n"
    )

    # Server hello, header block, data block, and one grouped terminal write. The
    # terminal write contains profile information, progress, the empty data block,
    # final progress, logs, profile events, and `EndOfStream`.
    assert native_send_count() - sends_before == 4


def test_nonempty_data_does_not_wait_for_interactive_delay():
    sends_before = native_send_count()
    assert (
        node.query(
            "SELECT number, sleepEachRow(0.25) FROM numbers(2)",
            settings={
                "compression": 1,
                "max_block_size": 1,
                "interactive_delay": 10_000_000,
            },
        )
        == "0\t0\n1\t0\n"
    )

    # Server hello, header block, two separately flushed data blocks, and the
    # grouped terminal write. Without the per-iteration `sync`, both rows remain
    # buffered until the terminal write because `interactive_delay` is ten seconds.
    assert native_send_count() - sends_before == 5


def test_insert_schema_and_completion_are_flushed():
    node.query("CREATE TABLE insert_target (x UInt64) ENGINE = Memory")
    sends_before = native_send_count()

    node.query(
        "INSERT INTO insert_target FORMAT TSV",
        stdin="1\n2\n",
        settings={"compression": 1},
        timeout=10,
    )

    # The schema must be visible before the client can encode its data. Completion
    # packets must then be visible before the client can finish the query.
    assert native_send_count() - sends_before == 4
    assert node.query("SELECT groupArray(x) FROM insert_target") == "[1,2]\n"

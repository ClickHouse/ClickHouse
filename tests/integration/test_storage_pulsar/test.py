import subprocess
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/macros.xml", "configs/remote_hosts.xml"],
    user_configs=["configs/users.xml"],
    with_pulsar=True,
)


@pytest.fixture(scope="module")
def pulsar_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_tables():
    yield
    instance.query("DROP TABLE IF EXISTS test.view SYNC")
    instance.query("DROP TABLE IF EXISTS test.consumer SYNC")
    instance.query("DROP TABLE IF EXISTS test.pulsar_writer SYNC")
    instance.query("DROP TABLE IF EXISTS test.pulsar_reader SYNC")


def pulsar_table(
    name, topic, group, extra_settings="", service_url="pulsar://pulsar1:6650"
):
    return f"""
        CREATE TABLE {name} (key UInt64, value UInt64)
        ENGINE = Pulsar
        SETTINGS pulsar_service_url = '{service_url}',
                 pulsar_topic_list = '{topic}',
                 pulsar_group_name = '{group}',
                 pulsar_format = 'JSONEachRow'{extra_settings}
    """


def wait_query_result(expected, query, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = instance.query(query)
        if TSV(result) == TSV(expected):
            return
        time.sleep(1)
    assert TSV(instance.query(query)) == TSV(expected)


def stop_pulsar(pulsar_cluster):
    # Give the broker time to shut down gracefully: the default 10s grace period
    # ends with SIGKILL, and a broker killed mid-write recovers much slower on
    # the next start.
    subprocess.check_call(["docker", "stop", "-t", "30", pulsar_cluster.pulsar_docker_id])


def start_pulsar(pulsar_cluster):
    subprocess.check_call(["docker", "start", pulsar_cluster.pulsar_docker_id])
    # On a loaded CI machine several standalone brokers restart concurrently and
    # a single restart can take well over two minutes.
    pulsar_cluster.wait_pulsar_is_available(max_retries=150)


def broker_is_healthy(pulsar_cluster):
    result = subprocess.run(
        [
            "docker",
            "exec",
            pulsar_cluster.pulsar_docker_id,
            "bin/pulsar-admin",
            "brokers",
            "healthcheck",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


@pytest.fixture(autouse=True)
def ensure_broker_is_available(pulsar_cluster):
    # A slow broker restart in one test (stop_pulsar/start_pulsar on a loaded CI
    # machine) must fail that test alone instead of cascading into every test
    # that runs after it: recheck the broker before each test and revive it if
    # it is not healthy.
    if not broker_is_healthy(pulsar_cluster):
        subprocess.check_call(["docker", "restart", "-t", "30", pulsar_cluster.pulsar_docker_id])
        pulsar_cluster.wait_pulsar_is_available(max_retries=150)


def test_experimental_gate(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    error = instance.query_and_get_error(
        pulsar_table("test.pulsar_reader", "gate_topic", "gate_group"),
        settings={"allow_experimental_pulsar_storage_engine": 0},
    )
    assert "SUPPORT_IS_DISABLED" in error

    full_attach = pulsar_table("test.pulsar_reader_full_attach", "gate_topic", "gate_group").replace(
        "CREATE TABLE", "ATTACH TABLE", 1
    )
    full_attach = full_attach.replace(
        "ATTACH TABLE test.pulsar_reader_full_attach",
        "ATTACH TABLE test.pulsar_reader_full_attach UUID '00000000-0000-0000-0000-000000000001'",
        1,
    )
    error = instance.query_and_get_error(full_attach, settings={"allow_experimental_pulsar_storage_engine": 0})
    assert "SUPPORT_IS_DISABLED" in error


def test_direct_select_requires_setting(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "select_gate_topic", "select_gate_group"))
    error = instance.query_and_get_error(
        "SELECT * FROM test.pulsar_reader",
        settings={"stream_like_engine_allow_direct_select": 0},
    )
    assert "QUERY_NOT_ALLOWED" in error


def test_direct_select_rejected_with_attached_mv(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "mv_guard_topic", "mv_guard_group"))
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    # The guard must not depend on the background streaming task being inside a
    # cycle: a direct SELECT must be rejected at any moment while a materialized
    # view is attached, including right after its creation and between cycles.
    for _ in range(5):
        error = instance.query_and_get_error("SELECT * FROM test.pulsar_reader")
        assert "QUERY_NOT_ALLOWED" in error
        time.sleep(0.3)

    # After the view is dropped, direct SELECT is allowed again.
    instance.query("DROP TABLE test.consumer SYNC")
    instance.query("SELECT * FROM test.pulsar_reader")


def test_dead_letter_queue_mode_rejected(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    error = instance.query_and_get_error(
        pulsar_table(
            "test.pulsar_reader",
            "dlq_topic",
            "dlq_group",
            extra_settings=", pulsar_handle_error_mode = 'dead_letter_queue'",
        )
    )
    assert "BAD_ARGUMENTS" in error


def test_dead_letter_queue_mode_rejected_on_full_attach(pulsar_cluster):
    # A full ATTACH supplies a brand-new engine definition, so the unsupported
    # mode must fail at DDL time exactly like CREATE, not later in streaming.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    full_attach = pulsar_table(
        "test.pulsar_reader_dlq_attach",
        "dlq_attach_topic",
        "dlq_attach_group",
        extra_settings=", pulsar_handle_error_mode = 'dead_letter_queue'",
    ).replace(
        "CREATE TABLE test.pulsar_reader_dlq_attach",
        "ATTACH TABLE test.pulsar_reader_dlq_attach UUID '00000000-0000-0000-0000-000000000002'",
        1,
    )
    error = instance.query_and_get_error(full_attach)
    assert "BAD_ARGUMENTS" in error


def test_csv_first_row_is_not_treated_as_header(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")

    def csv_table(name, group):
        return f"""
            CREATE TABLE {name} (key String, value String)
            ENGINE = Pulsar
            SETTINGS pulsar_service_url = 'pulsar://pulsar1:6650',
                     pulsar_topic_list = 'csv_header_topic',
                     pulsar_group_name = '{group}',
                     pulsar_format = 'CSV'
        """

    instance.query(csv_table("test.pulsar_reader", "csv_header_group"))
    instance.query(csv_table("test.pulsar_writer", "csv_header_writer_group"))
    instance.query(
        """
        CREATE TABLE test.view (key String, value String)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    # The first row coincides with the column names: with CSV header
    # autodetection left enabled it would be silently dropped as a header.
    instance.query("INSERT INTO test.pulsar_writer VALUES ('key', 'value'), ('a', 'b')")

    wait_query_result("a\tb\nkey\tvalue", "SELECT key, value FROM test.view ORDER BY key")


def test_system_stop_start_streaming(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "stop_start_topic", "stop_start_group"))
    instance.query(pulsar_table("test.pulsar_writer", "stop_start_topic", "stop_start_writer_group"))
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    instance.query("SYSTEM STOP test.pulsar_reader")

    num_rows = 10
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number FROM numbers({num_rows})"
    )

    # While the streaming is stopped nothing must reach the target table.
    time.sleep(5)
    assert instance.query("SELECT count() FROM test.view") == "0\n"

    instance.query("SYSTEM START test.pulsar_reader")

    expected = "\n".join(f"{i}\t{i}" for i in range(num_rows))
    wait_query_result(expected, "SELECT key, value FROM test.view ORDER BY key")


def test_produce_consume_via_materialized_view(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "mv_topic", "mv_group"))
    instance.query(pulsar_table("test.pulsar_writer", "mv_topic", "writer_group"))
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    num_rows = 50
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number * number FROM numbers({num_rows})"
    )

    expected = "\n".join(f"{i}\t{i * i}" for i in range(num_rows))
    wait_query_result(expected, "SELECT key, value FROM test.view ORDER BY key")


def test_attach_while_broker_down_recovers(pulsar_cluster):
    # A table attached while Pulsar is unreachable has no consumers; the background
    # initialization task must keep retrying and pick up consumption automatically
    # once the broker is available again, without a manual DETACH/ATTACH.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "outage_attach_topic", "outage_attach_group"))
    instance.query(
        pulsar_table("test.pulsar_writer", "outage_attach_topic", "outage_attach_writer_group")
    )
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    instance.query("DETACH TABLE test.pulsar_reader")
    stop_pulsar(pulsar_cluster)
    try:
        # ATTACH must succeed even though subscribing fails.
        instance.query("ATTACH TABLE test.pulsar_reader")
    finally:
        start_pulsar(pulsar_cluster)

    num_rows = 10
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number FROM numbers({num_rows})"
    )

    expected = "\n".join(f"{i}\t{i}" for i in range(num_rows))
    wait_query_result(expected, "SELECT key, value FROM test.view ORDER BY key")


def test_direct_select_rejected_while_consumers_not_ready(pulsar_cluster):
    # A table attached while Pulsar is unreachable has no consumers until the
    # background initialization task recreates them. A direct SELECT in that
    # window must fail with CANNOT_CONNECT_PULSAR instead of returning an empty
    # result set, which would make a broker outage indistinguishable from an
    # empty topic.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "select_outage_topic", "select_outage_group"))

    instance.query("DETACH TABLE test.pulsar_reader")
    stop_pulsar(pulsar_cluster)
    try:
        instance.query("ATTACH TABLE test.pulsar_reader")
        error = instance.query_and_get_error("SELECT * FROM test.pulsar_reader")
        assert "CANNOT_CONNECT_PULSAR" in error
    finally:
        start_pulsar(pulsar_cluster)

    # Once the broker is back and the consumers are recreated, direct SELECT
    # works again.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        result = instance.query_and_get_answer_with_error("SELECT * FROM test.pulsar_reader")
        if "CANNOT_CONNECT_PULSAR" not in result[1]:
            break
        time.sleep(1)
    instance.query("SELECT * FROM test.pulsar_reader")


def test_streaming_resumes_after_broker_restart(pulsar_cluster):
    # A broker outage in the middle of streaming must not leave the table
    # permanently stalled: consumption must resume once the broker is back.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "outage_stream_topic", "outage_stream_group"))
    instance.query(
        pulsar_table("test.pulsar_writer", "outage_stream_topic", "outage_stream_writer_group")
    )
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    num_rows = 10
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number FROM numbers({num_rows})"
    )
    expected = "\n".join(f"{i}\t{i}" for i in range(num_rows))
    wait_query_result(expected, "SELECT key, value FROM test.view ORDER BY key")

    stop_pulsar(pulsar_cluster)
    try:
        # Let the streaming task observe the outage.
        time.sleep(5)
    finally:
        start_pulsar(pulsar_cluster)

    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number + {num_rows}, number + {num_rows} FROM numbers({num_rows})"
    )
    # Reconnection of both the producer and the consumers can take a while on
    # slow (sanitizer) builds, so use a generous deadline. Messages interrupted
    # by the outage may be redelivered, so tolerate duplicate rows.
    expected = "\n".join(f"{i}\t{i}" for i in range(2 * num_rows))
    wait_query_result(
        expected, "SELECT DISTINCT key, value FROM test.view ORDER BY key", timeout=240
    )


def test_direct_select(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    # The subscription is created together with the table, so only messages
    # published after this point are delivered to it.
    instance.query(
        pulsar_table(
            "test.pulsar_reader",
            "select_topic",
            "select_group",
            extra_settings=", pulsar_commit_on_select = 1",
        )
    )
    instance.query(pulsar_table("test.pulsar_writer", "select_topic", "select_writer_group"))

    num_rows = 20
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number FROM numbers({num_rows})"
    )

    # A direct SELECT reads at most one batch per consumer, so accumulate the
    # rows over multiple queries. With `pulsar_commit_on_select = 1` returned
    # messages are acknowledged, so every row is seen at least once and
    # duplicates are possible only on redelivery.
    seen = set()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and len(seen) < num_rows:
        result = instance.query("SELECT key, value FROM test.pulsar_reader")
        for line in result.strip().splitlines():
            seen.add(line)
        time.sleep(0.2)
    expected = {f"{i}\t{i}" for i in range(num_rows)}
    assert seen == expected


def test_experimental_gate_allows_attach(pulsar_cluster):
    # The gate is two-sided: CREATE is blocked when the setting is disabled,
    # but an existing table must still ATTACH / load without it, otherwise
    # disabling the setting would strand already created tables.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "gate_attach_topic", "gate_attach_group"))

    instance.query("DETACH TABLE test.pulsar_reader")
    instance.query(
        "ATTACH TABLE test.pulsar_reader",
        settings={"allow_experimental_pulsar_storage_engine": 0},
    )
    assert instance.query("EXISTS TABLE test.pulsar_reader") == "1\n"
    instance.query("SELECT * FROM test.pulsar_reader")


def test_topic_list_required(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    error = instance.query_and_get_error(
        """
        CREATE TABLE test.pulsar_reader (key UInt64, value UInt64)
        ENGINE = Pulsar
        SETTINGS pulsar_service_url = 'pulsar://pulsar1:6650',
                 pulsar_group_name = 'no_topic_group',
                 pulsar_format = 'JSONEachRow'
        """
    )
    assert "NUMBER_OF_ARGUMENTS_DOESNT_MATCH" in error


def test_consume_compressed_messages(pulsar_cluster):
    # Messages produced by external Pulsar clients with Zstd or Snappy
    # compression must be readable: the client library decompresses them in
    # `uncompressMessageIfNeeded` only when it is built with these codecs.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "compression_topic", "compression_group"))
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    def produce(compression, message):
        # `pulsar-client produce` cannot compress, so use `pulsar-perf` with a
        # single-line payload file, which sends exactly that payload.
        subprocess.check_call(
            [
                "docker",
                "exec",
                pulsar_cluster.pulsar_docker_id,
                "bash",
                "-c",
                f"echo '{message}' > /tmp/payload_{compression}"
                f" && bin/pulsar-perf produce -m 1 -r 1 -f /tmp/payload_{compression}"
                f" -z {compression} compression_topic",
            ]
        )

    produce("ZSTD", '{"key":0,"value":0}')
    produce("SNAPPY", '{"key":1,"value":1}')

    # Delivery is at-least-once: a slow acknowledgement can put an already
    # written message onto the redelivery path, so tolerate duplicate rows.
    expected = "\n".join(f"{i}\t{i}" for i in range(2))
    wait_query_result(expected, "SELECT DISTINCT key, value FROM test.view ORDER BY key")


def test_num_consumers_zero_rejected(pulsar_cluster):
    # `pulsar_num_consumers = 0` would create a table with no consumers at all
    # (and `num_consumers` is a divisor in the block size calculation), so it
    # must be rejected up front at CREATE TABLE time.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    error = instance.query_and_get_error(
        pulsar_table(
            "test.pulsar_reader",
            "zero_consumers_topic",
            "zero_consumers_group",
            extra_settings=", pulsar_num_consumers = 0",
        )
    )
    assert "BAD_ARGUMENTS" in error


def test_num_consumers_above_limit_rejected(pulsar_cluster):
    # An unreasonably large `pulsar_num_consumers` would make CREATE eagerly
    # open that many consumers (an easy server-side DoS from a typoed value),
    # so it is capped by max(number of CPU cores, 16), like `kafka_num_consumers`.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    error = instance.query_and_get_error(
        pulsar_table(
            "test.pulsar_reader",
            "too_many_consumers_topic",
            "too_many_consumers_group",
            extra_settings=", pulsar_num_consumers = 100000",
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "pulsar_num_consumers" in error


def test_batch_size_zero_rejected(pulsar_cluster):
    # `pulsar_max_block_size = 0` would make every source stop after its first
    # (empty) loop iteration, and `pulsar_poll_max_batch_size = 0` would be
    # passed straight into the client's batch receive policy, so both must be
    # rejected up front at CREATE TABLE time.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    for setting in ("pulsar_max_block_size", "pulsar_poll_max_batch_size"):
        error = instance.query_and_get_error(
            pulsar_table(
                "test.pulsar_reader",
                "zero_batch_topic",
                "zero_batch_group",
                extra_settings=f", {setting} = 0",
            )
        )
        assert "BAD_ARGUMENTS" in error


def test_batch_size_above_int_max_rejected(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    for setting in ("pulsar_max_block_size", "pulsar_poll_max_batch_size"):
        error = instance.query_and_get_error(
            pulsar_table(
                "test.pulsar_reader",
                "large_batch_topic",
                "large_batch_group",
                extra_settings=f", {setting} = 2147483648",
            )
        )
        assert "BAD_ARGUMENTS" in error
        assert setting in error


def test_http_service_url_rejected(pulsar_cluster):
    # The bundled Pulsar client has no HTTP transport. Reject HTTP lookup URLs
    # before constructing it instead of retrying an unsupported configuration.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    for scheme in ("http", "https"):
        error = instance.query_and_get_error(
            pulsar_table(
                "test.pulsar_reader",
                "http_service_url_topic",
                "http_service_url_group",
                service_url=f"{scheme}://pulsar1:8080",
            )
        )
        assert "BAD_ARGUMENTS" in error
        assert "pulsar://" in error


def test_max_rows_per_message_zero_rejected(pulsar_cluster):
    # `MessageQueueSink` advances the row only inside the per-message loop, so with
    # `pulsar_max_rows_per_message = 0` an `INSERT` into a row-based format would spin
    # forever without ever consuming a row instead of failing.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    error = instance.query_and_get_error(
        pulsar_table(
            "test.pulsar_writer",
            "zero_rows_topic",
            "zero_rows_group",
            extra_settings=", pulsar_max_rows_per_message = 0",
        )
    )
    assert "BAD_ARGUMENTS" in error


def test_remote_host_filter_applies_to_service_url(pulsar_cluster):
    # The engine dials the brokers of `pulsar_service_url`, so the address must pass the
    # server's outbound allowlist (`configs/remote_hosts.xml` allows only this cluster's
    # broker), like every other remote surface. Otherwise anyone allowed to create a table
    # could make the server connect to an arbitrary host.
    instance.query("CREATE DATABASE IF NOT EXISTS test")

    def create(service_url):
        return instance.query_and_get_error(
            f"""
            CREATE TABLE test.pulsar_reader (key UInt64, value UInt64)
            ENGINE = Pulsar
            SETTINGS pulsar_service_url = '{service_url}',
                     pulsar_topic_list = 'host_filter_topic',
                     pulsar_group_name = 'host_filter_group',
                     pulsar_format = 'JSONEachRow'
            """
        )

    assert "UNACCEPTABLE_URL" in create("pulsar://not-allowed-host:6650")
    # A different port on the allowed host is a different entry in the allowlist.
    assert "UNACCEPTABLE_URL" in create("pulsar://pulsar1:6651")
    # Every broker of a multi-broker service URL is checked, not only the first one.
    assert "UNACCEPTABLE_URL" in create("pulsar://pulsar1:6650,not-allowed-host:6650")
    # An address without a scheme is not a valid service URL.
    assert "BAD_ARGUMENTS" in create("pulsar1:6650")

    # The allowed broker of this cluster still works.
    instance.query(
        pulsar_table("test.pulsar_reader", "host_filter_topic", "host_filter_group")
    )


def test_macros_expansion(pulsar_cluster):
    # The broker-facing string settings support macro substitution the same way
    # the other message-broker engines do: server-defined macros in the service
    # URL and the special {database}/{table} macros in the topic list and the
    # subscription name. The writer uses the already expanded literals, so
    # messages flow end-to-end only if the reader expanded its settings too.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(
        """
        CREATE TABLE test.pulsar_reader (key UInt64, value UInt64)
        ENGINE = Pulsar
        SETTINGS pulsar_service_url = 'pulsar://{pulsar_host}:{pulsar_port}',
                 pulsar_topic_list = '{database}_macro_topic',
                 pulsar_group_name = '{database}_{table}_macro_group',
                 pulsar_format = 'JSONEachRow'
        """
    )
    instance.query(
        pulsar_table("test.pulsar_writer", "test_macro_topic", "macro_writer_group")
    )
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    num_rows = 10
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number FROM numbers({num_rows})"
    )

    expected = "\n".join(f"{i}\t{i}" for i in range(num_rows))
    wait_query_result(expected, "SELECT key, value FROM test.view ORDER BY key")


def test_aborted_select_redelivers_prefetched_messages(pulsar_cluster):
    # A direct SELECT that stops early (LIMIT) leaves both uncommitted returned
    # messages and a prefetched unread tail of the current batch on the consumer.
    # `rollback` must put all of them onto the redelivery path instead of keeping
    # them attached to the pooled consumer, so nothing is lost or stuck.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(
        pulsar_table(
            "test.pulsar_reader",
            "abort_select_topic",
            "abort_select_group",
            extra_settings=", pulsar_commit_on_select = 1",
        )
    )
    instance.query(pulsar_table("test.pulsar_writer", "abort_select_topic", "abort_select_writer_group"))

    num_rows = 20
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number FROM numbers({num_rows})"
    )

    # Wait until the batch is actually polled, then abort after one row: the
    # rest of the batch is negatively acknowledged and must be redelivered.
    deadline = time.monotonic() + 120
    aborted = False
    while time.monotonic() < deadline and not aborted:
        result = instance.query("SELECT key, value FROM test.pulsar_reader LIMIT 1")
        aborted = bool(result.strip())
        time.sleep(0.2)
    assert aborted, "The aborted SELECT never received a message"

    # Negative acknowledgement redelivery uses the client default delay (60s),
    # so allow a deadline well above it. Every published row must eventually be
    # returned despite the aborted query.
    seen = set()
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline and len(seen) < num_rows:
        result = instance.query("SELECT key, value FROM test.pulsar_reader")
        for line in result.strip().splitlines():
            seen.add(line)
        time.sleep(1)
    expected = {f"{i}\t{i}" for i in range(num_rows)}
    assert seen == expected


def test_event_timestamp_virtual_columns(pulsar_cluster):
    # `_timestamp` / `_timestamp_ms` must expose the producer-set event
    # timestamp, not the broker publish timestamp, and stay NULL for messages
    # whose producer did not set it. `pulsar-client produce` cannot set the
    # event timestamp, so publish through the broker's REST producer, which
    # accepts an explicit `eventTime` per message.
    # The REST producer does not create the topic on demand, so create it
    # before the engine's consumers get a chance to auto-create it.
    subprocess.check_call(
        [
            "docker",
            "exec",
            pulsar_cluster.pulsar_docker_id,
            "bin/pulsar-admin",
            "topics",
            "create",
            "persistent://public/default/event_ts_topic",
        ]
    )
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "event_ts_topic", "event_ts_group"))
    instance.query(
        """
        CREATE TABLE test.view
        (key UInt64, value UInt64, ts Nullable(DateTime), ts_ms Nullable(DateTime64(3)))
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value, _timestamp AS ts, _timestamp_ms AS ts_ms FROM test.pulsar_reader
        """
    )
    event_time_ms = 1690000000123
    body = (
        '{"valueSchema": "{\\"type\\":\\"STRING\\",\\"schema\\":\\"\\",\\"properties\\":{}}",'
        ' "messages": ['
        f'{{"payload": "{{\\"key\\":0,\\"value\\":0}}", "eventTime": {event_time_ms}}},'
        ' {"payload": "{\\"key\\":1,\\"value\\":1}"}]}'
    )
    subprocess.check_call(
        [
            "docker",
            "exec",
            pulsar_cluster.pulsar_docker_id,
            "curl",
            "-sf",
            "-X",
            "POST",
            "http://localhost:8080/topics/persistent/public/default/event_ts_topic",
            "-H",
            "Content-Type: application/json",
            "-d",
            body,
        ]
    )

    # Delivery is at-least-once, so tolerate duplicate rows.
    expected = f"0\t0\t1690000000\t{event_time_ms}\n1\t1\t\\N\t\\N"
    wait_query_result(
        expected,
        """
        SELECT DISTINCT key, value, toUnixTimestamp(ts), toUnixTimestamp64Milli(ts_ms)
        FROM test.view ORDER BY key
        """,
    )


def test_table_comment_is_preserved(pulsar_cluster):
    # The engine must thread the `COMMENT` clause into its storage metadata the
    # same way the other message-broker engines do; otherwise `SHOW CREATE TABLE`
    # and `system.tables.comment` silently come back empty after create or attach.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(
        pulsar_table("test.pulsar_reader", "comment_topic", "comment_group")
        + " COMMENT 'Pulsar comment'"
    )

    def check():
        assert (
            instance.query(
                "SELECT comment FROM system.tables WHERE database = 'test' AND name = 'pulsar_reader'"
            )
            == "Pulsar comment\n"
        )
        # The client returns the statement TSV-escaped, so the quotes arrive as `\'`.
        assert "COMMENT \\'Pulsar comment\\'" in instance.query(
            "SHOW CREATE TABLE test.pulsar_reader"
        )

    check()
    instance.query("DETACH TABLE test.pulsar_reader")
    instance.query("ATTACH TABLE test.pulsar_reader")
    check()


def test_empty_messages_are_skipped(pulsar_cluster):
    # Zero-length messages carry no rows for the input format, so the consumer
    # skips over them (acknowledging each) until it finds a payload. The skip
    # must be iterative: a single poll can return a whole batch of empty
    # messages, and recursing once per skipped message could exhaust the stack.
    # The REST producer does not create the topic on demand, so create it
    # before the engine's consumers get a chance to auto-create it.
    subprocess.check_call(
        [
            "docker",
            "exec",
            pulsar_cluster.pulsar_docker_id,
            "bin/pulsar-admin",
            "topics",
            "create",
            "persistent://public/default/empty_msgs_topic",
        ]
    )
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(
        pulsar_table(
            "test.pulsar_reader",
            "empty_msgs_topic",
            "empty_msgs_group",
            extra_settings=", pulsar_poll_max_batch_size = 1000",
        )
    )
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    # A long run of empty messages followed by a single real one, published in
    # one batch through the broker's REST producer (`pulsar-client produce`
    # rejects an empty `-m`). With `pulsar_poll_max_batch_size = 1000` a single
    # poll can return the entire run.
    empty_messages = ", ".join('{"payload": ""}' for _ in range(300))
    body = (
        '{"valueSchema": "{\\"type\\":\\"STRING\\",\\"schema\\":\\"\\",\\"properties\\":{}}",'
        f' "messages": [{empty_messages},'
        ' {"payload": "{\\"key\\":7,\\"value\\":7}"}]}'
    )
    subprocess.check_call(
        [
            "docker",
            "exec",
            pulsar_cluster.pulsar_docker_id,
            "curl",
            "-sf",
            "-X",
            "POST",
            "http://localhost:8080/topics/persistent/public/default/empty_msgs_topic",
            "-H",
            "Content-Type: application/json",
            "-d",
            body,
        ]
    )

    # Delivery is at-least-once, so tolerate duplicate rows.
    wait_query_result("7\t7", "SELECT DISTINCT key, value FROM test.view")


def test_remote_host_filter_applies_to_lookup_results(pulsar_cluster):
    # The service URL is validated when the table is created, but the client
    # also follows lookup responses and dials the returned broker addresses
    # directly, so those physical addresses must pass `remote_url_allow_hosts`
    # too - otherwise a redirecting broker could steer the server to a host
    # outside the allowlist. Tighten the allowlist after the table is created
    # and sever the connections: every reconnect goes through a fresh lookup,
    # and its result (this very broker) is now disallowed.
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(
        pulsar_table(
            "test.pulsar_reader",
            "lookup_filter_topic",
            "lookup_filter_group",
            extra_settings=", pulsar_commit_on_select = 1",
        )
    )

    def produce(message):
        # Produce with the broker-local CLI client: the table engine's own
        # producer would be cut off by the very filter under test. The client
        # splits `-m` on the separator (a comma by default, which JSON payloads
        # contain, and interpreted as a Java regex), so pick a non-special
        # character that does not occur in the message.
        subprocess.check_call(
            [
                "docker",
                "exec",
                pulsar_cluster.pulsar_docker_id,
                "bin/pulsar-client",
                "produce",
                "lookup_filter_topic",
                "-s",
                ";",
                "-m",
                message,
            ]
        )

    def read_rows():
        # While the connections are cut the pooled consumers can be dropped and
        # recreated in the background, in which case a direct SELECT reports
        # CANNOT_CONNECT_PULSAR instead of returning an empty set.
        result, _ = instance.query_and_get_answer_with_error(
            "SELECT key, value FROM test.pulsar_reader"
        )
        return result

    def wait_row_seen(row, timeout):
        seen = set()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and row not in seen:
            for line in read_rows().splitlines():
                seen.add(line.strip())
            time.sleep(0.5)
        assert row in seen

    # Baseline: messages flow while the broker is allowed.
    produce('{"key":1,"value":1}')
    wait_row_seen("1\t1", 120)

    allowed_config = """<clickhouse>
    <remote_url_allow_hosts>
        <host>pulsar1:6650</host>
    </remote_url_allow_hosts>
</clickhouse>"""
    blocked_config = allowed_config.replace("pulsar1:6650", "some-other-host:6650")
    config_path = "/etc/clickhouse-server/config.d/remote_hosts.xml"
    try:
        instance.replace_config(config_path, blocked_config)
        instance.query("SYSTEM RELOAD CONFIG")
        # The allowlist is checked per connection attempt, so the connections
        # established while the broker was allowed keep working: sever them.
        stop_pulsar(pulsar_cluster)
        start_pulsar(pulsar_cluster)
        instance.wait_for_log_line(
            "Rejected a connection to the Pulsar broker", timeout=120
        )
        produce('{"key":2,"value":2}')
        # The message must not reach the table while the broker is disallowed.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert "2\t2" not in read_rows()
            time.sleep(1)
    finally:
        instance.replace_config(config_path, allowed_config)
        instance.query("SYSTEM RELOAD CONFIG")

    # Once the allowlist permits the broker again, the client's reconnect
    # attempts start passing the filter and consumption resumes.
    wait_row_seen("2\t2", 240)

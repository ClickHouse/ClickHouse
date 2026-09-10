import pytest
import socket
import uuid
import threading
import time

from helpers.cluster import ClickHouseCluster
from helpers.port_forward import PortForward
from helpers.postgres_utility import get_postgres_conn
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn(cluster.postgres_ip, cluster.postgres_port)
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS postgres_database")
        cursor.execute("CREATE DATABASE postgres_database")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def setup_infinite_query(started_cluster):
    # Connect to postgres_database database
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute(
        """CREATE OR REPLACE FUNCTION generate_infinite_sequence(start_from INT DEFAULT 1)
RETURNS SETOF INT AS $$
DECLARE
    counter INT := start_from;
BEGIN
    LOOP
        RETURN NEXT counter;
        counter := counter + 1;
        PERFORM pg_sleep(0.01);
    END LOOP;
END;
$$ LANGUAGE plpgsql;"""
    )
    cursor.execute(
        """CREATE OR REPLACE VIEW infinite_counter AS
SELECT generate_infinite_sequence() as counter;"""
    )

    postgres_host_with__port = (
        f"{started_cluster.postgres_ip}:{started_cluster.postgres_port}"
    )
    yield cursor, postgres_host_with__port
    # Cleanup
    cursor.close()
    conn.close()


@pytest.fixture(scope="module")
def setup_sleepy_view(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute(
        """CREATE OR REPLACE FUNCTION sleepy_start()
RETURNS SETOF integer AS $$
BEGIN
    PERFORM pg_sleep(600);
    RETURN NEXT 1;
END;
$$ LANGUAGE plpgsql;"""
    )
    cursor.execute(
        """CREATE OR REPLACE VIEW sleepy_view AS
SELECT sleepy_start() AS id;"""
    )

    yield

    cursor.close()
    conn.close()


@pytest.fixture(scope="module")
def setup_streaming_view(started_cluster):
    """A view whose COPY starts streaming at once but keeps running for a long time.

    `infinite_counter` cannot be used for this: it is a plpgsql `SETOF` function, so
    PostgreSQL builds its whole result set before the first row and never sends
    `CopyOutResponse`. `pqxx::stream_from`'s constructor then blocks and `onStart` never
    regains control, which is a different window from the one under test here.

    The leading bulk rows make PostgreSQL send `CopyOutResponse` immediately, so the
    constructor returns; the sleeping tail keeps the COPY running long enough to cancel it. A
    single fast row is not enough, because `CopyOutResponse` is not sent until there is a
    buffer of output to send with it.
    """
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute(
        """CREATE TABLE streaming_head AS
SELECT g AS counter, repeat('y', 900) AS pad FROM generate_series(1, 20000) g;"""
    )
    cursor.execute(
        """CREATE OR REPLACE VIEW streaming_counter AS
SELECT counter FROM streaming_head
UNION ALL
SELECT g FROM generate_series(1, 600) g
CROSS JOIN LATERAL (SELECT pg_sleep(0.5) WHERE g IS NOT NULL) s;"""
    )

    yield

    cursor.execute("DROP VIEW IF EXISTS streaming_counter")
    cursor.execute("DROP TABLE IF EXISTS streaming_head")
    cursor.close()
    conn.close()


@pytest.fixture(scope="module")
def setup_big_data_table(started_cluster):
    # Connect to postgres_database database
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute(
        """CREATE TABLE big_data_table (
    id SERIAL PRIMARY KEY,
    random_int INT,
    random_string VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )
    cursor.execute(
        """INSERT INTO big_data_table (random_int, random_string)
SELECT
    floor(random() * 1000000)::INT as random_int,
    substring(md5(random()::text || clock_timestamp()::text) from 1 for 50) as random_string
FROM generate_series(1, 1000000);
        """
    )
    postgres_host_with__port = (
        f"{started_cluster.postgres_ip}:{started_cluster.postgres_port}"
    )
    yield cursor, postgres_host_with__port
    # Cleanup
    cursor.close()
    conn.close()


def wait_for_port_forward_connection(port_forward):
    for _ in range(50):
        with port_forward._clients_lock:
            if port_forward._clients:
                return
        time.sleep(0.1)

    raise AssertionError("No active PostgreSQL proxy connection")


def wait_for_proxy_listener_closed(host, port):
    for _ in range(50):
        try:
            with socket.create_connection((host, port), timeout=0.1):
                pass
        except OSError:
            return
        time.sleep(0.1)

    raise AssertionError("PostgreSQL proxy listener is still accepting connections")


class StatementStallingProxy:
    """Forwards the PostgreSQL wire protocol, but withholds the first client statement that
    contains `marker` from the server until release() is called.

    Stalling `BEGIN READ ONLY` pins the reading source inside `pqxx::ReadTransaction`'s
    constructor, before `onStart` has published `tx`. Stalling the `COPY` pins it one step
    later, inside `pqxx::stream_from`'s constructor, with `tx` published and still no
    statement running on the connection. Those are the two startup windows in which a cancel
    request to the server finds no statement to interrupt.

    `skip` exists because the source's transaction is not the only one on the wire. Reading
    through `postgresql()` without an explicit column list first fetches the table structure,
    which opens its own read transaction. Stalling that earlier one pins the query in
    analysis instead, where it is killed before a pipeline exists and the source never runs.
    """

    BEGIN = b"BEGIN READ ONLY"
    COPY = b"COPY ("

    def __init__(self, skip=0, marker=BEGIN):
        self._marker = marker
        self._skip = skip
        self._seen = 0
        self._seen_lock = threading.Lock()
        self._release = threading.Event()
        self._stalled = threading.Event()
        self._sock = None
        self._threads = []
        self._stop = False

    def _should_stall(self):
        # Connections are pumped by independent threads, so the counter is shared state.
        with self._seen_lock:
            self._seen += 1
            return self._seen > self._skip

    def _pump(self, source, destination, is_client_to_server):
        stalled_once = False
        # TCP is a stream, so the marker can straddle two reads. Keep the last few bytes of the
        # previous one to search across the boundary.
        carry = b""
        while not self._stop:
            try:
                data = source.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                break

            seen_marker = False
            if is_client_to_server and not stalled_once:
                seen_marker = self._marker in carry + data
                keep = len(self._marker) - 1
                carry = (carry + data)[-keep:] if keep else b""

            if seen_marker:
                stalled_once = True
                if self._should_stall():
                    self._stalled.set()
                    # Hold the statement back. The bounded wait keeps a failure surfacing as
                    # an assertion in the test body rather than as a hung worker.
                    self._release.wait(timeout=60)

            try:
                destination.sendall(data)
            except OSError:
                break

        for s in (source, destination):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _serve(self):
        while not self._stop:
            try:
                downstream, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                upstream.connect(self._address)
            except OSError:
                downstream.close()
                continue

            downstream.settimeout(1)
            upstream.settimeout(1)
            for args in ((downstream, upstream, True), (upstream, downstream, False)):
                t = threading.Thread(target=self._pump, args=args, daemon=True)
                self._threads.append(t)
                t.start()

    def start(self, address):
        self._address = address
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("", 0))
        self._sock.listen()
        self._sock.settimeout(1)
        self._runner = threading.Thread(target=self._serve, daemon=True)
        self._runner.start()
        return self._sock.getsockname()[1]

    def wait_until_stalled(self, timeout=60):
        if not self._stalled.wait(timeout=timeout):
            raise AssertionError(f"proxy never saw {self._marker.decode()}")

    def release(self):
        self._release.set()

    def stop(self):
        self._stop = True
        self._release.set()
        if self._sock:
            self._sock.close()
        for t in self._threads:
            t.join(timeout=5)


class ResponseStallingProxy(StatementStallingProxy):
    """Forwards the `CopyOutResponse` and withholds every row after it, keeping both sockets open.
    The source is then blocked in `read_row()` with nothing buffered to drain.
    """

    def __init__(self):
        super().__init__(marker=StatementStallingProxy.COPY)
        self._copy_sent = threading.Event()

    def _pump(self, source, destination, is_client_to_server):
        carry = b""
        buf = bytearray()
        stalled_once = False
        while not self._stop:
            try:
                data = source.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                break

            try:
                if is_client_to_server:
                    if not self._copy_sent.is_set() and self._marker in carry + data:
                        self._copy_sent.set()
                    carry = (carry + data)[-(len(self._marker) - 1) :]
                    destination.sendall(data)
                    continue

                if stalled_once or not self._copy_sent.is_set():
                    destination.sendall(data)
                    continue

                # The response starts at a message boundary. Forward its first message only.
                buf += data
                if len(buf) < 5 or len(buf) < 1 + int.from_bytes(buf[1:5], "big"):
                    continue
                first = 1 + int.from_bytes(buf[1:5], "big")
                destination.sendall(bytes(buf[:first]))
                stalled_once = True
                self._stalled.set()
                self._release.wait(timeout=60)
                destination.sendall(bytes(buf[first:]))
            except OSError:
                break

        for s in (source, destination):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def test_kill_query_while_transaction_is_starting(started_cluster, setup_infinite_query):
    """A cancel arriving while the transaction is still being constructed must not be lost.

    In that window `onStart` has not published `tx`, so `onCancel` cannot reach the connection
    and only raises `stop_requested`. `onStart` has to honour that flag once the transaction is
    published, instead of proceeding into the blocking COPY, or the cancellation is dropped and
    the query keeps running until PostgreSQL finishes it on its own.
    """
    _, _ = setup_infinite_query
    proxy = StatementStallingProxy(marker=StatementStallingProxy.BEGIN)
    port = proxy.start((started_cluster.postgres_ip, started_cluster.postgres_port))
    proxy_host = socket.gethostbyname(socket.gethostname())
    query_id = str(uuid.uuid4())
    query_errors = []

    # An engine table with a declared structure, created before the proxy starts stalling, so
    # that the read is the only thing that opens a transaction (see StatementStallingProxy).
    node1.query("DROP TABLE IF EXISTS stalled_counter")
    node1.query(
        f"""CREATE TABLE stalled_counter (counter Nullable(Int32))
ENGINE = PostgreSQL(
    '{proxy_host}:{port}',
    'postgres_database',
    'infinite_counter',
    'postgres',
    'ClickHouse_PostgreSQL_P@ssw0rd')"""
    )

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            "SELECT * FROM stalled_counter",
            query_id=query_id,
            timeout=120,
        )
        query_errors.append(error)

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        try:
            # The source is now pinned inside the transaction constructor, before `tx` is
            # published.
            proxy.wait_until_stalled()

            assert_eq_with_retry(
                node1,
                f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
                "1",
                retry_count=60,
                sleep_time=0.5,
            )

            node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
            # Let the cancel be delivered to the source while it is still stalled, so that
            # `onCancel` runs with `tx` still null and consumes the flag.
            assert_eq_with_retry(
                node1,
                f"SELECT is_cancelled FROM system.processes WHERE query_id='{query_id}'",
                "1",
                retry_count=60,
                sleep_time=0.5,
            )
        finally:
            proxy.release()

        # The recheck must abandon the read. Without it the source enters the COPY and the
        # query runs on against an endless view, so this join times out.
        query_thread.join(timeout=60)
        assert (
            not query_thread.is_alive()
        ), "cancelled query kept running after the transaction started"
        assert query_errors and "QUERY_WAS_CANCELLED" in query_errors[0], query_errors

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
            "0",
            retry_count=60,
            sleep_time=0.5,
        )
    finally:
        # A failed assertion must not leave the read, the proxy or the table behind for the
        # tests that run after this one. Tear the proxy down before joining: a second KILL QUERY
        # can find the source already streaming, where `onCancel` leaves the connection to the
        # pipeline thread, and dropping the connection is then what ends the endless COPY.
        node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC", ignore_error=True)
        proxy.stop()
        query_thread.join(timeout=60)
        node1.query("DROP TABLE IF EXISTS stalled_counter")
        assert not query_thread.is_alive(), "query thread outlived the test"


def test_kill_query_while_copy_is_starting(started_cluster, setup_streaming_view):
    """A cancel arriving after the transaction is published but before the COPY starts must
    not be lost either.

    This is one step later than `test_kill_query_while_transaction_is_starting`: `tx` is
    published but no statement runs yet, so a cancel request to the server would be dropped.
    The failed `COPY` start must still surface as a cancellation, not a transport error.

    Stalling the COPY request rather than the `BEGIN` is what places the cancel in this
    window; `test_kill_query_while_transaction_is_starting` cannot reach it, because there
    the source is still pinned before publication.

    The view has to start streaming promptly (see `setup_streaming_view`), otherwise
    `stream_from`'s constructor never returns and the source is pinned before this window
    instead of inside it.
    """
    proxy = StatementStallingProxy(marker=StatementStallingProxy.COPY)
    port = proxy.start((started_cluster.postgres_ip, started_cluster.postgres_port))
    proxy_host = socket.gethostbyname(socket.gethostname())
    query_id = str(uuid.uuid4())
    query_errors = []

    node1.query("DROP TABLE IF EXISTS copy_stalled_counter")
    node1.query(
        f"""CREATE TABLE copy_stalled_counter (counter Nullable(Int32))
ENGINE = PostgreSQL(
    '{proxy_host}:{port}',
    'postgres_database',
    'streaming_counter',
    'postgres',
    'ClickHouse_PostgreSQL_P@ssw0rd')"""
    )

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            "SELECT * FROM copy_stalled_counter",
            query_id=query_id,
            timeout=120,
        )
        query_errors.append(error)

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        try:
            # The source has published `tx` and is now pinned inside the `stream_from`
            # constructor, with the COPY request withheld from the server.
            proxy.wait_until_stalled()

            assert_eq_with_retry(
                node1,
                f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
                "1",
                retry_count=60,
                sleep_time=0.5,
            )

            # Delivered while the COPY is still withheld, so the connection has no statement in
            # progress. The query ends at once, so there is no cancelled state to observe.
            node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
        finally:
            proxy.release()

        # Without the fix the dropped cancel leaves the source streaming to the end of the view.
        query_thread.join(timeout=60)
        assert (
            not query_thread.is_alive()
        ), "cancelled query kept running after the COPY started"
        assert query_errors and "QUERY_WAS_CANCELLED" in query_errors[0], query_errors

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
            "0",
            retry_count=60,
            sleep_time=0.5,
        )
    finally:
        node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC", ignore_error=True)
        proxy.stop()
        query_thread.join(timeout=60)
        node1.query("DROP TABLE IF EXISTS copy_stalled_counter")
        assert not query_thread.is_alive(), "query thread outlived the test"


def test_kill_query_while_the_read_is_stalled(started_cluster, setup_streaming_view):
    """A cancel arriving while the COPY is streaming must not wait for the server.
    The proxy stays stalled across the kill, so only the transport itself can end the read.
    """
    proxy = ResponseStallingProxy()
    port = proxy.start((started_cluster.postgres_ip, started_cluster.postgres_port))
    proxy_host = socket.gethostbyname(socket.gethostname())
    query_id = str(uuid.uuid4())
    query_errors = []

    node1.query("DROP TABLE IF EXISTS read_stalled_counter")
    node1.query(
        f"""CREATE TABLE read_stalled_counter (counter Nullable(Int32))
ENGINE = PostgreSQL(
    '{proxy_host}:{port}',
    'postgres_database',
    'streaming_counter',
    'postgres',
    'ClickHouse_PostgreSQL_P@ssw0rd')"""
    )

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            "SELECT * FROM read_stalled_counter",
            query_id=query_id,
            timeout=120,
        )
        query_errors.append(error)

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        proxy.wait_until_stalled()

        # Nothing is buffered after `CopyOutResponse`, so from here the source blocks in the read.
        node1.wait_for_log_line(f"{query_id}.*Generate a chunk from stream")

        node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")

        # Still stalled: without the fix the read waits for as long as the peer stays silent.
        query_thread.join(timeout=30)
        assert (
            not query_thread.is_alive()
        ), "cancelled query kept waiting on a silent connection"
        assert query_errors and "QUERY_WAS_CANCELLED" in query_errors[0], query_errors
    finally:
        node1.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC", ignore_error=True)
        proxy.stop()
        query_thread.join(timeout=60)
        node1.query("DROP TABLE IF EXISTS read_stalled_counter")
        assert not query_thread.is_alive(), "query thread outlived the test"


def test_kill_query_when_postgresql_cancel_connection_fails(
    started_cluster, setup_sleepy_view
):
    port_forward = PortForward()
    port = port_forward.start(
        (started_cluster.postgres_ip, started_cluster.postgres_port)
    )
    proxy_host = socket.gethostbyname(socket.gethostname())
    query_id = str(uuid.uuid4())
    query_errors = []
    query_exceptions = []

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                f"""SELECT count() FROM postgresql(
        '{proxy_host}:{port}',
        'postgres_database',
        'sleepy_view',
        'postgres',
        'ClickHouse_PostgreSQL_P@ssw0rd')""",
                query_id=query_id,
                timeout=60,
            )
            query_errors.append(error)
        except Exception as ex:
            query_exceptions.append(ex)

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
            "1",
            retry_count=60,
            sleep_time=0.5,
        )
        wait_for_port_forward_connection(port_forward)

        # Keep the data connection open but refuse new ones. The kill must get by without one.
        port_forward.stop()
        wait_for_proxy_listener_closed(proxy_host, port)

        node1.query(f"KILL QUERY WHERE query_id='{query_id}'")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
            "0",
            retry_count=60,
            sleep_time=0.5,
        )
        assert node1.query("SELECT 1").strip() == "1"
    finally:
        port_forward.stop(force=True)

    query_thread.join(timeout=30)
    assert not query_thread.is_alive()
    assert not query_exceptions
    assert query_errors


def test_kill_infinite_query(setup_infinite_query):
    cursor, postgres_host_with__port = setup_infinite_query
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            f"""SELECT * FROM postgresql(
        '{postgres_host_with__port}',
        'postgres_database',
        'infinite_counter',
        'postgres',
        'ClickHouse_PostgreSQL_P@ssw0rd')""",
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Stream data from database")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    # Verify that query was successfully cancelled in PostgreSQL server
    cursor.execute(
        """SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
and query = 'COPY (SELECT "counter" FROM "infinite_counter") TO STDOUT';
        """
    )
    assert cursor.fetchall()[0][0] == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_kill_query_during_generation(setup_big_data_table):
    cursor, postgres_host_with__port = setup_big_data_table
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            f"""SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM postgresql(
    '{postgres_host_with__port}',
    'postgres_database',
    'big_data_table',
    'postgres',
    'ClickHouse_PostgreSQL_P@ssw0rd'
)
SETTINGS max_block_size = 10000""",
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Generate a chunk from stream")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    # Verify that query was successfully cancelled in PostgreSQL server
    cursor.execute(
        """SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
and query = 'COPY (SELECT "counter" FROM "infinite_counter") TO STDOUT';
        """
    )
    assert cursor.fetchall()[0][0] == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_cancel_infinite_query(setup_infinite_query):
    _, postgres_host_with__port = setup_infinite_query
    query_id = str(uuid.uuid4())

    def execute_query():
        query = f"""SELECT * FROM postgresql(
        '{postgres_host_with__port}',
        'postgres_database',
        'infinite_counter',
        'postgres',
        'ClickHouse_PostgreSQL_P@ssw0rd')"""
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""/usr/bin/clickhouse client --query_id "{query_id}" --query "{query}" """,
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()
    # Wait for the query to start by polling system.processes for its unique query_id.
    # Robust against log timing: a one-shot log line can be written before a
    # look_behind_lines=0 tail is installed, turning the stale-log flake into a
    # missed-line timeout.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
        "1",
        retry_count=60,
        sleep_time=0.5,
    )
    time.sleep(2)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")


def test_cancel_query_during_generation(setup_big_data_table):
    _, postgres_host_with__port = setup_big_data_table

    def execute_query():
        query = f"""SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM postgresql(
    '{postgres_host_with__port}',
    'postgres_database',
    'big_data_table',
    'postgres',
    'ClickHouse_PostgreSQL_P@ssw0rd'
)
SETTINGS max_block_size = 10000"""
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""/usr/bin/clickhouse client --query "{query}" """,
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()
    # Use look_behind_lines=0 to only match new log lines, avoiding stale matches
    # from preceding tests in this file (test_kill_query also produces this line).
    node1.wait_for_log_line("Generate a chunk from stream", look_behind_lines=0)
    time.sleep(2)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")

"""Reproductions of open MergeTree transaction defects.

Each test states the behaviour the server does not have, so each fails today and carries
`xfail(strict=True, raises=AssertionError)`. Fixing a defect turns its test into a failure of the
suite, which is the signal to drop the decorator and keep the test as a regression test.

`raises` is what makes the suite mean anything, and it only works if nothing but the defect raises
an `AssertionError`. Without `raises`, `_pytest.skipping` absorbs any exception in any phase into
`xfail`, and a module that executed no test body at all still reports twelve clean expected
failures. With it, an `AssertionError` is the expected failure -- so everything that is not the
defect raises `Precondition` instead: setup, the state a failpoint was supposed to create,
positive controls, and cleanup.

Every request carries an explicit timeout. `Instance.query` defaults to `DEFAULT_QUERY_TIMEOUT`
(600 s) and `requests` with `timeout=None` waits forever, either of which outlasts every budget
here, and a polling loop whose own requests are unbounded has no bound. A request left running in
a worker thread keeps pytest alive at exit whatever `shutdown(wait=False)` says, because
`concurrent.futures` joins its non-daemon workers through an atexit hook. A hung module is worse
than a failed reproduction: it reports nothing.

The node is private to this module because these reproductions cannot share a server: the
failpoints are global, and the removal of a part whose creation has not committed raises a
`LOGICAL_ERROR` that aborts a debug or sanitizer build.
"""

import concurrent.futures
import contextlib
import functools
import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/transactions.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_zookeeper=True,
    # Transactions refuse to start unless Keeper advertises these.
    keeper_required_feature_flags=[
        "filtered_list",
        "multi_read",
        "list_with_stat_and_data",
        "check_stat",
    ],
)

QUERY_TIMEOUT = 120
# Let a request fail on its own first, so its error reaches the test instead of a `TimeoutError`.
FUTURE_TIMEOUT = QUERY_TIMEOUT + 30
# Long enough for every bounded request a worker can still be inside.
JOIN_TIMEOUT = FUTURE_TIMEOUT
RESTART_TIMEOUT = 180
# An upper bound on the two hang reproductions. They do not usually spend it: a mutation that
# cannot select its part says so in `system.mutations`, and that reason -- not the clock -- is what
# identifies the defect.
HANG_TIMEOUT = 30
# A request gets at least this long whatever a polling loop has left of its budget.
MIN_REQUEST_TIMEOUT = 5

# `TransactionManager` reads `transaction_log.zookeeper_path`, default `/clickhouse/txn`, and
# `configs/transactions.xml` does not override it.
TXN_LOG_PATH = "/clickhouse/txn/log"

# `TransactionManager::commitTransaction` logs this when it postpones finalization because it does
# not know whether the commit landed. It is the only observable that separates an unknown-state
# commit from an ordinary one, since the CSN entry is written before the fault is injected.
UNKNOWN_STATE_LOG = "will finalize it later"
# The exception a removal of an uncommitted creation raises today.
CREATION_CSN_LOG = "creation_csn is not set"
# `PostponeReasons::VERSION_NOT_VISIBLE` as `system.mutations.parts_postpone_reasons` carries it
# (`MergeTreeMutationStatus.h`). Part selection tests visibility at the mutation's start CSN
# (`StorageMergeTree::getDataPartsToMutate`), so this reason is the mutation defect itself rather
# than a server that happens to be slow.
VERSION_NOT_VISIBLE = "Not visible by transaction version"
# `PostponeReasons::NO_FREE_THREADS`, written by a pass that stops before it looks at any part
# (`mt_select_parts_to_mutate_no_free_threads`). `require_fresh_mutation_pass` uses it to date a
# reason: the map is rebuilt from scratch on every pass.
NO_FREE_THREADS = "No free threads in pool"
# `version_metadata_store_creation_csn_fail` fires inside `~MergeTreeData::Transaction`, which
# logs the exception and swallows it. The statement already reported the first injected failure,
# so this line is the only thing that says the rollback's own mark failed too.
# `VersionMetadata::storeCreationCSN` names the part it was about to write
# (`VersionInfo::getObjectName`), so the table and the part turn this global message into one
# about the reproduction at hand.
ROLLBACK_MARK_LOG = "Injected failure of the creation_csn store of"

# `non_transactional_removal_store_fail_after_first_part` raises this from
# `NonTransactionalRemovalLocks::store`, after the first part of the batch was already stamped.
INJECTED_REMOVAL_STORE = "Injected failure of the removal store of the part"

FAILPOINTS = [
    "non_transactional_removal_store_fail_after_first_part",
    "non_transactional_drop_pause_before_publish",
    "transaction_force_unknown_state_after_commit",
    "transaction_log_pause_before_loading_entries",
    "version_metadata_store_creation_csn_fail",
    "version_metadata_store_pause_before_rename",
    "mt_select_parts_to_mutate_no_free_threads",
]


class Precondition(Exception):
    """The setup for a reproduction did not hold, or cleanup after it did not.

    Deliberately not an `AssertionError`: `xfail(raises=AssertionError)` reports this as a real
    failure instead of the expected one, so a reproduction that never reached its defect cannot
    pass for one that did.
    """


def require(condition, message):
    if not condition:
        raise Precondition(message)


def q(query, timeout=QUERY_TIMEOUT, **kwargs):
    return node.query(query, timeout=timeout, **kwargs)


def q_error(query, timeout=QUERY_TIMEOUT, **kwargs):
    return node.query_and_get_error(query, timeout=timeout, **kwargs)


def poll(predicate, timeout, step=0.2):
    """True once `predicate` holds, False at `timeout`.

    `predicate` takes the seconds to allow its request, so that a single stalled poll cannot
    outlast the budget this loop is enforcing. The value is the time left, floored at
    `MIN_REQUEST_TIMEOUT`: handing a request a few hundred milliseconds would fail it on the clock
    rather than answer the question. So the loop returns within `timeout`, and the last request
    inside it within `MIN_REQUEST_TIMEOUT` of that.
    """
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if predicate(max(MIN_REQUEST_TIMEOUT, remaining)):
            return True
        time.sleep(step)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        # `SET TRANSACTION SNAPSHOT` refuses a CSN in the reserved range, and a fresh server sits
        # at the top of it. A commit that changes nothing allocates no CSN, so insert a row.
        q("CREATE TABLE t_warmup (n UInt64) ENGINE = MergeTree ORDER BY n")
        q("BEGIN TRANSACTION; INSERT INTO t_warmup VALUES (1); COMMIT;")
        latest = int(q("SELECT transactionLatestSnapshot()").strip())
        if latest <= 32:
            raise RuntimeError(f"latest snapshot {latest} is still a reserved CSN")
        yield cluster
    finally:
        # `test_drop_partition_of_uncommitted_creation_is_refused` reproduces a `LOGICAL_ERROR`,
        # and the abort it causes in a debug or sanitizer build is part of what it reports.
        cluster.shutdown(ignore_logical_errors=True, ignore_fatal=True)


# `cluster.shutdown` can exempt a run from its `LOGICAL_ERROR` and `<Fatal>` checks, but only the
# whole run, and this module needs both exemptions for one reproduction: in a debug or sanitizer
# build its logical error aborts, and the abort writes a whole stack of fatal lines of which one
# carries the message, so counting cannot tell that stack from an unrelated one. The exemptions are
# therefore replaced per test. A test named here may log the message it names and may abort over
# it; every other test must log no logical error and no fatal line of its own.
LOGICAL_ERROR_EXPECTED = {
    "test_drop_partition_of_uncommitted_creation_is_refused": CREATION_CSN_LOG,
}
# The watchdog logs the `SIGKILL` of `restart_server` at fatal level (`BaseDaemon.cpp`), which is
# this module killing a server on purpose. `cluster.shutdown` exempts the same line.
EXPECTED_FATAL = "Child process was terminated by signal 9 (KILL)"


@pytest.fixture(autouse=True)
def isolated_test(start_cluster, request):
    """Each test gets its own transaction sessions and leaves no failpoint armed.

    Cleanup raises rather than logs: a failpoint still armed or a transaction still open makes
    every later reproduction meaningless, and a teardown error is reported as an error instead of
    being absorbed into the next test's expected failure.
    """
    global _session_epoch, _pending_requests
    require(not _contaminated, f"an earlier test left this server unusable: {_contaminated}")
    _session_epoch += 1
    _session_names.clear()
    _started_sessions.clear()
    _query_ids.clear()
    _pending_requests = 0
    expected_error = LOGICAL_ERROR_EXPECTED.get(request.node.name)
    logical_errors = log_hits("LOGICAL_ERROR")
    expected_errors = log_hits(expected_error) if expected_error else 0
    fatals = log_hits("<Fatal>")
    expected_fatals = log_hits(EXPECTED_FATAL)
    yield
    # pytest runs the tests after this one whatever a teardown error says, so a server that
    # cannot be cleaned is replaced rather than merely reported.
    if not server_is_up():
        try:
            restart_server()
        except Exception as e:
            _mark_contaminated(f"the server stopped answering and would not restart: {e}")
            raise
        raise Precondition("the server stopped answering during the test")
    left_behind = []
    for failpoint in FAILPOINTS:
        try:
            # Disabling also releases whatever is parked at a pauseable failpoint.
            q(f"SYSTEM DISABLE FAILPOINT {failpoint}", timeout=60)
        except Exception as e:
            left_behind.append(f"failpoint {failpoint} armed: {e}")
    for query_id in sorted(_query_ids):
        try:
            q(f"KILL QUERY WHERE query_id = '{query_id}' SYNC FORMAT Null", timeout=60)
        except Exception as e:
            left_behind.append(f"query {query_id} still running: {e}")
    # A transaction left open by a failed assertion would hold a snapshot and pin part cleanup.
    for name in sorted(_session_names):
        try:
            # Without `session_check`: a test may have restarted the server on purpose, and a
            # session the restart took with it has nothing left to roll back.
            _, error = tx_in_answer_with_error(name, "ROLLBACK", timeout=60, check=False)
        except Exception as e:
            left_behind.append(f"session {name} unreachable: {e}")
            continue
        # A session that never began a transaction answers `INVALID_TRANSACTION`, which is the
        # ordinary case; anything else means the session is still held or still in a transaction.
        if error and "INVALID_TRANSACTION" not in error:
            left_behind.append(f"session {name} not rolled back: {error}")
    if _pending_requests:
        # A restart cannot cancel the client thread, but it does take away the session and the
        # state its next request would have reached.
        left_behind.append(f"{_pending_requests} request(s) outlived the test")
    if left_behind:
        try:
            restart_server()
        except Exception as e:
            left_behind.append(f"and the restart failed: {e}")
            _mark_contaminated("; ".join(left_behind))
        raise Precondition("cleanup left the server contaminated: " + "; ".join(left_behind))
    unaccounted = (log_hits("LOGICAL_ERROR") - logical_errors) - (
        (log_hits(expected_error) - expected_errors) if expected_error else 0
    )
    require(
        unaccounted <= 0,
        f"the server logged {unaccounted} logical error(s) this test did not declare",
    )
    if request.node.name not in LOGICAL_ERROR_EXPECTED:
        unaccounted_fatal = (log_hits("<Fatal>") - fatals) - (
            log_hits(EXPECTED_FATAL) - expected_fatals
        )
        require(
            unaccounted_fatal <= 0,
            f"the server logged {unaccounted_fatal} fatal line(s) during this test",
        )


def _mark_contaminated(reason):
    global _contaminated
    _contaminated = reason


def server_is_up():
    try:
        q("SELECT 1", timeout=30)
        return True
    except Exception:
        return False


def restart_server(timeout=RESTART_TIMEOUT):
    """Restarts the server under a deadline this module enforces.

    `Instance.restart_clickhouse` enforces none. `stop_clickhouse` waits on `llvm-symbolizer`
    with no deadline at all if the process outlives its stop budget, and logs a failed kill
    instead of reporting it. `wait_start` runs ten 30 s readiness attempts before it looks at its
    budget -- and `start_clickhouse` honours `wait_start=False` only on the branch where no
    process is left, so a stop that did not take puts that batch straight back in the path, and a
    surviving old server would be accepted as the restarted one. Kill it here, prove it is gone,
    then poll readiness.
    """
    # Every session goes with the process, and the next request in one has to create it again.
    _started_sessions.clear()
    # What `stop_clickhouse(kill=True)` does before killing; a SIGKILL skips the flush at exit.
    # Outside the budget on purpose: under a coverage build this single request may take 300 s.
    node.flush_per_test_coverage()
    deadline = time.monotonic() + timeout
    killed = node.exec_in_container(
        ["bash", "-c", "pkill -9 clickhouse; echo rc=$?"], user="root"
    ).strip()
    require(
        killed.endswith("rc=0") or killed.endswith("rc=1"),
        f"pkill did not run: {killed!r}",
    )
    require(
        poll(lambda _: server_process_count() == 0, 30),
        "the server survived SIGKILL, so the restart would have waited on the old process",
    )
    node.start_clickhouse(start_wait_sec=30, wait_start=False)

    def answers(remaining):
        try:
            return q("SELECT 1", timeout=remaining).strip() == "1"
        except Exception:
            return False

    remaining = deadline - time.monotonic()
    require(remaining > 0, f"stopping and launching used the whole {timeout} s restart budget")
    if not poll(answers, remaining):
        raise Precondition(f"the server did not answer within {timeout} s of a restart")
    # `wait_start` does this, and skipping it would attribute this module's coverage elsewhere.
    node.arm_per_test_coverage()


def server_process_count():
    """How many server processes the container has.

    `Instance.get_process_pid` returns `None` both for "no match" and for a probe that did not
    run, which would read as a dead server. Here a probe that did not run raises.
    """
    out = node.exec_in_container(
        ["bash", "-c", "pgrep -c clickhouse || true"], user="root"
    ).strip()
    require(out.isdigit(), f"the process probe did not run: {out!r}")
    return int(out)


def restart_if_down():
    if not server_is_up():
        restart_server()


_session_epoch = 0
_session_names = set()
# Sessions that have had a request, so the next one can insist the session is still there.
_started_sessions = set()
# `default_session_timeout` is 60 s (`HTTPHandler.cpp`), well inside the waits here. An expired
# session is dropped with its transaction rolled back, and the next request silently gets a new
# one: the reproduction would then read a table outside the transaction it believes it is in and
# report the result as the defect. `max_session_timeout` is 3600 s, the largest value allowed
# without server configuration.
SESSION_TIMEOUT = 3600
_query_ids = set()
# Set when cleanup could not establish a clean server. The next test refuses to run rather than
# reproduce anything against it.
_contaminated = ""
# Requests that outlived their test. A restart cannot cancel the client thread, so teardown has to
# know they exist.
_pending_requests = 0


def session_id(session):
    name = f"session_{_session_epoch}_{session}"
    _session_names.add(name)
    return name


def session_params(name, check=True):
    """Query parameters that keep a session alive and refuse to replace it silently.

    `session_check` makes a request whose session is gone fail with `SESSION_NOT_FOUND` instead of
    opening a new one, so a lost transaction is reported rather than reproduced. The first request
    of a session has to create it, and a restart destroys every session, so `restart_server`
    forgets them.
    """
    params = {"session_id": name, "session_timeout": SESSION_TIMEOUT}
    if check and name in _started_sessions:
        params["session_check"] = 1
    _started_sessions.add(name)
    return params


def tx_in(name, query, query_id=None, timeout=QUERY_TIMEOUT):
    """Runs `query` in a named session, so `BEGIN`/`COMMIT` span calls. Raises on a server error."""
    params = session_params(name)
    if query_id is not None:
        params["query_id"] = query_id
        _query_ids.add(query_id)
    return node.http_query(None, data=query, params=params, timeout=timeout)


def tx_in_answer_with_error(name, query, timeout=QUERY_TIMEOUT, check=True):
    return node.http_query_and_get_answer_with_error(
        None, data=query, params=session_params(name, check), timeout=timeout
    )


def tx(session, query, query_id=None, timeout=QUERY_TIMEOUT):
    return tx_in(session_id(session), query, query_id=query_id, timeout=timeout)


def tx_answer_with_error(session, query, timeout=QUERY_TIMEOUT):
    return tx_in_answer_with_error(session_id(session), query, timeout=timeout)


def in_session(session, *queries, query_id=None):
    """A callable for a worker thread, with the session name resolved now.

    `session_id` builds the name from the epoch current when it is called, so a worker that
    outlives its test would send its next request into the following test's session.
    """
    name = session_id(session)

    def run():
        return [tx_in(name, query, query_id=query_id) for query in queries]

    return run


def in_session_answer_with_error(session, query):
    name = session_id(session)
    return functools.partial(tx_in_answer_with_error, name, query)


class Workers:
    """A thread pool that reports a request outliving its test.

    Every request here is bounded, so a worker still running after `JOIN_TIMEOUT` means something
    else is wrong -- and it would both reach into the next test and hang pytest at exit.
    """

    def __init__(self, max_workers):
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._futures = []

    def submit(self, fn, *args, **kwargs):
        future = self._pool.submit(fn, *args, **kwargs)
        self._futures.append(future)
        return future

    def close(self):
        _, pending = concurrent.futures.wait(self._futures, timeout=JOIN_TIMEOUT)
        self._pool.shutdown(wait=False)
        return pending


@contextlib.contextmanager
def workers(max_workers):
    pool = Workers(max_workers)
    failed = False
    try:
        yield pool
    except BaseException:
        failed = True
        raise
    finally:
        global _pending_requests
        pending = pool.close()
        _pending_requests += len(pending)
        if pending and not failed:
            # Raising here would replace the exception a failing test is reporting, so this only
            # speaks up when the test body itself had nothing to say. Either way teardown sees
            # `_pending_requests` and replaces the server.
            raise Precondition(f"{len(pending)} request(s) outlived the test")
        if pending:
            logging.warning("%d request(s) outlived a failing test", len(pending))


def settled(query_id, future, timeout):
    """`(finished, started)` -- whether the statement finished, and whether it was ever running.

    A request still in flight is not in `system.processes` yet, so an unsynchronised poll would
    read "finished" from a statement that never started. `started` is handed back because a
    statement that never started is not a hang either, whatever the clock says.
    """
    started = False

    def check(remaining):
        nonlocal started
        if future.done():
            return True
        if running(query_id, remaining) > 0:
            started = True
            return False
        return started

    return poll(check, timeout), started


def running(query_id, timeout=QUERY_TIMEOUT):
    return int(
        q(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'",
            timeout=timeout,
        ).strip()
    )


def mutation_postpone_reasons(table, timeout=QUERY_TIMEOUT):
    return q(
        "SELECT arrayStringConcat(mapValues(parts_postpone_reasons), '|') FROM system.mutations "
        f"WHERE database = currentDatabase() AND table = '{table}' AND NOT is_done",
        timeout=timeout,
    ).strip()


def mutation_outcome(table, future, timeout=HANG_TIMEOUT, stop_early=True):
    """`(finished, postponed)` for a mutation that should finish.

    With `stop_early` this returns as soon as either is settled -- the request answered, or the
    scheduler recorded that it cannot select a part because the version is not visible to the
    mutation's transaction -- so only an inconclusive run pays the whole budget.

    A verdict passes `stop_early=False` and waits the budget out. `selectPartsToMutate` clears
    `current_parts_postpone_reasons` at the start of each pass, so the reason outlives whatever
    made it true: read right after releasing the thing the mutation was waiting for, it describes
    the pass before that and would report a mutation that is about to proceed as stuck.
    """
    postponed = False

    def check(remaining):
        nonlocal postponed
        if future.done():
            return True
        if not stop_early:
            return False
        if VERSION_NOT_VISIBLE in mutation_postpone_reasons(table, remaining):
            postponed = True
            return True
        return False

    poll(check, timeout)
    if not stop_early and not future.done():
        # The budget above is spent; this last read gets a bound of its own, not `QUERY_TIMEOUT`.
        postponed = VERSION_NOT_VISIBLE in mutation_postpone_reasons(table, MIN_REQUEST_TIMEOUT)
    return future.done(), postponed


def require_fresh_mutation_pass(table, future, timeout=60):
    """Makes the scheduler prove it ran a mutation selection pass for `table` now.

    `selectPartsToMutate` clears `current_parts_postpone_reasons` at the start of every pass, so a
    postponement read later says only that some pass recorded it -- a reason justified by a
    failpoint that has since been released survives until the next pass. `NO_FREE_THREADS` comes
    from a pass that stops before it looks at any part, so seeing it dates the map to after this
    call, and whatever the verdict reads afterwards comes from a pass later still.

    A mutation that has already finished has nothing to date; `system.mutations` drops it too.
    """
    with failpoints("mt_select_parts_to_mutate_no_free_threads"):
        require(
            poll(
                lambda remaining: future.done()
                or NO_FREE_THREADS in mutation_postpone_reasons(table, remaining),
                timeout,
            ),
            f"the scheduler ran no mutation selection pass for {table} within {timeout} s, so a "
            "postponement read after this would not describe the state under test",
        )


def require_cancelled(future, what):
    """The killed request must fail because it was killed, not for some other reason.

    A success here means the statement finished between the observation that it was stuck and the
    kill, so it was not stuck and the caller's conclusion about it does not hold.
    """
    try:
        future.result(timeout=FUTURE_TIMEOUT)
    except Exception as e:
        require(
            "QUERY_WAS_CANCELLED" in str(e) or "Cancelled" in str(e),
            f"{what} failed for a reason other than the kill: {e}",
        )
        return
    raise Precondition(f"{what} succeeded after the kill, so it had not stopped making progress")


def kill_query(query_id):
    q(f"KILL QUERY WHERE query_id = '{query_id}' SYNC FORMAT Null")


def wait_for_no_merges(timeout=60):
    if not poll(
        lambda remaining: q(
            "SELECT count() FROM system.merges", timeout=remaining
        ).strip()
        == "0",
        timeout,
    ):
        raise Precondition(
            "merges are still running: "
            + q(
                "SELECT database, table FROM system.merges",
                timeout=MIN_REQUEST_TIMEOUT,
            )
        )


def wait_for_no_cleanup(table=None, timeout=60):
    """Waits out the cleanup rounds that were already running when cleanup was stopped.

    `SYSTEM STOP CLEANUP` cancels an `ActionBlocker`, and `IMergeTreeCleanupThread::run` tests it
    once, before `iterate`: a round past that test goes on to `grabOldParts` and removes parts
    after the statement returned. A later round returns at the test, so waiting for the task to
    stop executing is enough. `system.background_schedule_pool` names the task of each table
    `<database>.<table> (CleanupThread)` (`IMergeTreeCleanupThread`).
    """
    if table:
        scope = f"= '{q('SELECT currentDatabase()').strip()}.{table} (CleanupThread)'"
    else:
        scope = "LIKE '% (CleanupThread)'"
    executing = (
        "SELECT count() FROM system.background_schedule_pool "
        f"WHERE executing AND log_name {scope}"
    )
    if not poll(
        lambda remaining: q(executing, timeout=remaining).strip() == "0", timeout
    ):
        raise Precondition(
            "a cleanup round is still running after it was stopped: "
            + q(
                "SELECT log_name, elapsed_ms FROM system.background_schedule_pool "
                f"WHERE executing AND log_name {scope}",
                timeout=MIN_REQUEST_TIMEOUT,
            )
        )


@contextlib.contextmanager
def merges_and_cleanup_stopped():
    """Stops every merge and the cleanup of outdated parts. The reproductions need both.

    The removal-store failpoint fires once, on the first batch of two or more parts to reach
    `NonTransactionalRemovalLocks::store`, and a merge or a part removal of any table -- a system
    log included -- is indistinguishable there, every caller passing `LockKind::REMOVAL`. A
    background cleanup that takes the failpoint first leaves the statement under test succeeding,
    which `require_injected_failure` then rejects.

    Cleanup also erases the reproduction itself. The rollback of the failed removal hands the empty
    covering parts to `removePartsFromWorkingSet` with `clear_without_timeout`, which sets
    `remove_time = 0`, so `grabOldParts` may delete them whatever `old_parts_lifetime` says. Once
    the directories are gone the reload finds only the original parts, keeps the rows, and the
    reproduction reports a defect that is still there. `SYSTEM STOP MERGES` does not cover this:
    cleanup runs on its own thread under `ActionLocks::Cleanup`.

    Enter this only once the test's tables exist. Neither statement covers a table created
    afterwards: without a table both iterate the tables of every database and take an action lock
    per table (`InterpreterSystemQuery::startStopAction`). `SYSTEM STOP MERGES` also leaves a merge
    that is already running alone, and `SYSTEM STOP CLEANUP` a cleanup round that is already past
    its cancellation test, hence the two waits.
    """
    q("SYSTEM STOP MERGES")
    q("SYSTEM STOP CLEANUP")
    try:
        wait_for_no_merges()
        wait_for_no_cleanup()
        yield
    finally:
        q("SYSTEM START CLEANUP")
        q("SYSTEM START MERGES")


def require_parts(table, expected):
    actual = int(
        q(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND active"
        ).strip()
    )
    require(
        actual == expected,
        f"{table} has {actual} active parts, expected {expected}: a merge got there first and "
        "the removal batch would not hold two parts",
    )


@contextlib.contextmanager
def failpoints(*names):
    for name in names:
        q(f"SYSTEM ENABLE FAILPOINT {name}")
    try:
        yield
    finally:
        for name in names:
            try:
                q(f"SYSTEM DISABLE FAILPOINT {name}", timeout=60)
            except Exception as e:
                logging.warning("could not disable failpoint %s: %s", name, e)


def require_injected_failure(error):
    """The statement must have failed through the injected store error, not another way.

    The failpoint fires once, on the first removal batch of two or more parts to reach it. A merge
    or another statement can take it first, and then the statement under test either succeeds or
    fails for an unrelated reason -- neither of which says anything about the defect.

    The message names the part the store was about to write, so it also says which call site fired.
    Matching that text and not just `CANNOT_WRITE_TO_FILE` keeps a genuine write failure elsewhere
    in the statement from passing as the injection.
    """
    require(
        error and INJECTED_REMOVAL_STORE in error,
        f"the injected store error did not reach the statement: {error!r}",
    )
    logging.info("injected store error: %s", one_line(error))


def one_line(text, limit=300):
    """`text` with its newlines collapsed, so one log line carries all of it."""
    return " ".join(text.split())[:limit] if text else text


def rollback_mark_log(table):
    """The log line the rollback's own failed mark leaves, for `table` alone.

    The failpoint is global and fires once for whichever metadata store reaches it first, so the
    bare message says nothing about which table's rollback failed.
    """
    return f"{ROLLBACK_MARK_LOG} {q('SELECT currentDatabase()').strip()}.{table}"


def require_rollback_mark_failed(table, baseline):
    """The rollback's own `creation_csn` mark must have failed too.

    `require_injected_failure` cannot tell the two injections apart: both raise
    `CANNOT_WRITE_TO_FILE`, and the statement reports only the first. Without this, the
    reproduction still passes its precondition when only the removal store failed, which is the
    single-fault test next door.

    `~MergeTreeData::Transaction` logs the second failure and swallows it. The wait is for the log
    file, not for the rollback: the local transaction object unwinds before the exception leaves
    the storage call, so the mark has already failed by the time the client sees the first error.
    """
    require(
        poll(lambda _: log_hits(rollback_mark_log(table)) > baseline, HANG_TIMEOUT),
        "the rollback's own creation_csn mark did not fail, so this is the single-fault path",
    )


def table_path(table):
    """The one directory the table's parts are in.

    A table on several disks would put them in several, and everything that reads the disk here
    would be reading one of them. These tables use the default policy, so this says so rather
    than taking the first path and hoping.
    """
    paths = q(
        "SELECT arrayStringConcat(data_paths, ' ') FROM system.tables "
        f"WHERE database = currentDatabase() AND name = '{table}'"
    ).split()
    require(len(paths) == 1, f"{table} has {len(paths)} data paths, expected one: {paths}")
    return paths[0]


def part_dirs(table):
    """The part directories on disk, `detached` excluded.

    `system.parts` cannot answer this: without `_state` in the SELECT it returns only Active and
    Outdated parts (`StoragesInfo::getParts`), and a rollback whose own mark failed leaves the empty
    covering parts PreActive.
    """
    listing = node.exec_in_container(
        ["bash", "-c", f"cd {table_path(table)} && ls -d */ 2>/dev/null"],
        user="root",
    )
    names = [line.rstrip("/") for line in listing.split()]
    return sorted(name for name in names if name != "detached")


def leftover_part_dirs(table):
    """Part directories on disk that no active part owns.

    After a failed non-transactional removal these are its empty covering parts. `StorageMergeTree
    ::removeRolledBackEmptyPartsAndRethrow` removes them inside the failing statement, so an empty
    result is the fixed behaviour and a non-empty one is what the loader would read as prehistoric.
    """
    active = q(
        "SELECT arrayStringConcat(groupArray(name), ' ') FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' AND active"
    ).split()
    leftover = sorted(set(part_dirs(table)) - set(active))
    logging.info("%s: active %s, leftover on disk %s", table, active, leftover)
    return leftover


def require_rolled_back_parts_on_disk(table):
    """The empty covering parts of the failed removal must still be on disk.

    They are what the reload loads, and nothing keeps them: the rollback set `remove_time = 0`, so
    cleanup may delete them at any moment (see `merges_and_cleanup_stopped`). Without them the
    reload keeps the rows and the test reports a fix that did not happen.

    A repair that makes the rollback remove the directories instead of marking them fails here,
    red rather than green: it satisfies the assertion below for a reason this reproduction cannot
    tell from cleanup having run. Such a repair needs this precondition rewritten, not only the
    xfail marker dropped.
    """
    require(
        leftover_part_dirs(table),
        f"nothing is left on disk for {table} beside its active parts: the rolled-back "
        "empty covering parts were removed before the reload could load them",
    )


def reload_state(table):
    """The parts, and what the loader would find on disk for them.

    The reload reproductions turn on one thing: the rollback of a failed non-transactional removal
    marks `creation_csn = RolledBackCSN` in memory only, because `VersionMetadataOnDisk` defers the
    store of a part that was never in a transaction. The empty covering parts keep their final
    directory names and no `txn_version.txt`, so the loader reads them as prehistoric. pytest prints
    this when a strict xfail passes, so a run that does not reproduce says which of those it lost.

    `_state` is selected to get the PreActive parts too, and the `txn_version*` files are printed
    with their contents: a file that exists says nothing on its own, the TIDs and CSNs in it do.
    """
    parts = q(
        "SELECT groupArray((name, _state, rows, active, visible, creation_csn, removal_csn)) "
        f"FROM system.parts WHERE database = currentDatabase() AND table = '{table}'"
    ).strip()
    detached = q(
        "SELECT groupArray((name, reason)) FROM system.detached_parts "
        f"WHERE database = currentDatabase() AND table = '{table}'"
    ).strip()
    path = table_path(table)
    versions = node.exec_in_container(
        [
            "bash",
            "-c",
            f"cd {path} && find . -maxdepth 3 -name 'txn_version*' "
            "-exec sh -c 'printf \"%s=[\" \"$1\"; tr \"\\n\" \" \" < \"$1\"; printf \"] \"' sh {} \\;",
        ],
        user="root",
    ).strip()
    return (
        f"parts {parts} | detached {detached} | dirs {' '.join(part_dirs(table))} "
        f"| txn_version {versions or 'none'}"
    )


def require_rows(table, expected):
    actual = q(f"SELECT count(), sum(n) FROM {table}").strip()
    require(actual == expected, f"{table} holds {actual!r} before the reload, expected {expected!r}")


def keeper_latest_csn(timeout=QUERY_TIMEOUT):
    """The highest CSN written to the transaction log in Keeper, loaded or not.

    Entry names are `csn-` plus the CSN padded to ten digits (`Tx::serializeCSN`).
    """
    return int(
        q(
            "SELECT max(toUInt64OrZero(substring(name, 5))) FROM system.zookeeper "
            f"WHERE path = '{TXN_LOG_PATH}'",
            timeout=timeout,
        ).strip()
    )


def log_hits(text, filename="clickhouse-server.log"):
    """How many times `text` appears in the server log so far.

    Presence alone proves nothing: one log serves the whole module, so a line left by an earlier
    test would answer for this one. Callers take a baseline and wait for it to grow.
    """
    return len(
        [
            line
            for line in node.grep_in_log(text, from_host=True, filename=filename).splitlines()
            if line.strip()
        ]
    )


def covered_active_parts(table):
    """Active parts that another active part of the same partition covers."""
    return q(
        "WITH p AS (SELECT partition_id, min_block_number mn, max_block_number mx, level, name"
        "           FROM system.parts"
        f"          WHERE database = currentDatabase() AND table = '{table}' AND active)"
        " SELECT covered.name FROM p AS covered, p AS covering"
        " WHERE covered.name != covering.name"
        "   AND covered.partition_id = covering.partition_id"
        "   AND covering.mn <= covered.mn AND covering.mx >= covered.mx"
        "   AND covering.level > covered.level"
        " ORDER BY covered.name"
    ).split()


def two_parts(
    table,
    settings="old_parts_lifetime = 3600, cleanup_delay_period = 1, max_cleanup_delay_period = 1",
):
    """Two parts of one row each, with the cleanup thread polling as fast as it can.

    The fast poll is the point: the empty covering parts a failed removal rolls back carry
    `remove_time = 0`, so a reproduction that forgets `SYSTEM STOP CLEANUP` loses them within a
    second or two and reports a fix that did not happen. At the default 30 s it would depend on how
    long the test took.
    """
    q(f"DROP TABLE IF EXISTS {table} SYNC")
    q(f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS {settings}")
    # Before the inserts: a merge of the two rows into one part leaves the removal batch holding a
    # single part, and the failpoint has no second part to fail after. The module-wide stop comes
    # later and cannot cover this, since it only locks the tables that exist when it runs.
    q(f"SYSTEM STOP MERGES {table}")
    q(f"INSERT INTO {table} VALUES (1)")
    q(f"INSERT INTO {table} VALUES (2)")


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124488: SET TRANSACTION SNAPSHOT accepts a snapshot above the latest one",
)
def test_snapshot_above_latest_is_refused(start_cluster):
    q("DROP TABLE IF EXISTS t_snapshot SYNC")
    q("CREATE TABLE t_snapshot (n UInt64) ENGINE = MergeTree ORDER BY n")
    q("INSERT INTO t_snapshot VALUES (1)")
    latest = int(q("SELECT transactionLatestSnapshot()").strip())

    tx(1, "BEGIN TRANSACTION")
    accepted, error = tx_answer_with_error(
        1, f"SET TRANSACTION SNAPSHOT {latest + 1000000}"
    )
    tx(1, "ROLLBACK")

    # The latest snapshot and the reserved ones stay acceptable.
    for snapshot in (latest, 1, 3):
        tx(2, "BEGIN TRANSACTION")
        tx(2, f"SET TRANSACTION SNAPSHOT {snapshot}")
        seen = tx(2, "SELECT count() FROM t_snapshot").strip()
        tx(2, "ROLLBACK")
        require(seen == "1", f"snapshot {snapshot} does not see the row: {seen}")

    require(
        not error or "INVALID_TRANSACTION" in error,
        f"the snapshot was refused for an unrelated reason: {error}",
    )
    assert error, (
        f"a snapshot {latest + 1000000} above the latest {latest} was accepted: {accepted!r}"
    )


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124488: a mutation never finishes at a raised snapshot, because part "
    "selection tests visibility at the start CSN",
)
def test_mutation_at_raised_snapshot_finishes(start_cluster):
    q("DROP TABLE IF EXISTS t_raised_snapshot SYNC")
    q(
        "CREATE TABLE t_raised_snapshot (n UInt64, v UInt64) ENGINE = MergeTree "
        "PARTITION BY n ORDER BY n"
    )
    q("INSERT INTO t_raised_snapshot VALUES (1, 0)")

    tx(1, "BEGIN TRANSACTION")
    tx(2, "BEGIN TRANSACTION")
    tx(2, "INSERT INTO t_raised_snapshot VALUES (2, 0)")
    tx(2, "COMMIT")

    latest = q("SELECT transactionLatestSnapshot()").strip()
    tx(1, f"SET TRANSACTION SNAPSHOT {latest}")
    seen = tx(1, "SELECT count() FROM t_raised_snapshot").strip()
    require(seen == "2", f"the raised snapshot does not see both rows: {seen}")

    tx(1, "SET mutations_sync = 1")
    query_id = "raised_snapshot_mutation"
    try:
        with workers(1) as pool:
            mutation = pool.submit(
                in_session(
                    1, "ALTER TABLE t_raised_snapshot UPDATE v = 1 WHERE 1", query_id=query_id
                )
            )
            finished, postponed = mutation_outcome(
                "t_raised_snapshot", mutation, stop_early=False
            )
            if finished:
                # Not suppressed: a statement that failed for another reason is not this defect.
                mutation.result(timeout=FUTURE_TIMEOUT)
            else:
                require(
                    postponed,
                    "the mutation neither finished nor was postponed for part visibility, so "
                    "this run shows nothing about the defect",
                )
                kill_query(query_id)
                require_cancelled(mutation, "the mutation")

        assert finished, (
            f"the mutation at a raised snapshot did not finish within {HANG_TIMEOUT} s"
        )

        tx(1, "COMMIT")
        assert q("SELECT n, v FROM t_raised_snapshot ORDER BY n") == "1\t1\n2\t1\n"
    finally:
        with contextlib.suppress(Exception):
            tx(1, "ROLLBACK")


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124488: a snapshot set to a commit the transaction log has not loaded is "
    "accepted, and the mutation that follows misses that commit's part",
)
def test_mutation_covers_part_of_unloaded_commit(start_cluster):
    q("DROP TABLE IF EXISTS t_unloaded_commit SYNC")
    q("DROP TABLE IF EXISTS t_unloaded_commit_poke SYNC")
    q(
        "CREATE TABLE t_unloaded_commit (n UInt64, v UInt64) ENGINE = MergeTree "
        "PARTITION BY n ORDER BY n"
    )
    q("INSERT INTO t_unloaded_commit VALUES (1, 0)")

    tx(1, "BEGIN TRANSACTION")
    latest = int(q("SELECT transactionLatestSnapshot()").strip())

    snapshot_id = "unloaded_commit_snapshot"
    mutation_id = "unloaded_commit_mutation"
    held = False
    try:
        q("SYSTEM ENABLE FAILPOINT transaction_log_pause_before_loading_entries")
        q("SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit")
        held = True

        unknown_state_lines = log_hits(UNKNOWN_STATE_LOG)

        with workers(2) as pool:
            try:
                commit = pool.submit(
                    in_session(
                        2,
                        "BEGIN TRANSACTION",
                        "INSERT INTO t_unloaded_commit VALUES (2, 0)",
                        "COMMIT",
                    )
                )

                q(
                    "SYSTEM WAIT FAILPOINT transaction_log_pause_before_loading_entries PAUSE",
                    timeout=60,
                )

                # The CSN entry is written before the fault is injected, so a Keeper entry alone does
                # not say the commit is in unknown state: a normally finalized commit that the paused
                # thread has not loaded passes that check too, and then the mutation below hangs on
                # the raised-snapshot defect (#124488) rather than on this one. The postponement log
                # line is what separates them.
                require(
                    poll(lambda _: log_hits(UNKNOWN_STATE_LOG) > unknown_state_lines, 60),
                    "the commit did not go into unknown state: the injected fault never fired",
                )
                q("SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit")

                # The updating thread wakes on its own, so the pause above can be reached before the
                # commit has written its entry.
                require(
                    poll(lambda remaining: keeper_latest_csn(remaining) > latest, 60),
                    f"the commit never reached Keeper: the log still ends at CSN {latest}",
                )
                loaded = int(q("SELECT transactionLatestSnapshot()").strip())
                require(
                    loaded == latest,
                    f"the held thread loaded the commit anyway: snapshot moved {latest} -> {loaded}",
                )

                # The held commit's CSN is the next one. Accepting it at once is the defect; a correct
                # server waits for the log to load it, or refuses it.
                snapshot = pool.submit(
                    in_session(
                        1, f"SET TRANSACTION SNAPSHOT {latest + 1}", query_id=snapshot_id
                    )
                )
                if not settled(snapshot_id, snapshot, HANG_TIMEOUT)[0]:
                    # The server waited for the log instead of accepting the snapshot, which is the
                    # repair. Release it and stop: what follows would hang on the raised-snapshot
                    # selection defect (#124488) and report this test as its own expected failure.
                    q("SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries")
                    held = False
                    snapshot.result(timeout=FUTURE_TIMEOUT)
                    raise Precondition(
                        "the snapshot was not accepted while unloaded, so #124488 is repaired: "
                        "drop this test's xfail marker and its premise. It cannot report the "
                        "repair as the expected failure turning into a pass, because everything "
                        "after this point measures a different defect"
                    )
                snapshot.result(timeout=FUTURE_TIMEOUT)

                tx(1, "SET mutations_sync = 1")
                mutation = pool.submit(
                    in_session(
                        1, "ALTER TABLE t_unloaded_commit UPDATE v = 1 WHERE 1", query_id=mutation_id
                    )
                )
                # Let the mutation reach the server before the pause is released. It returns as
                # soon as the statement finished or the scheduler said why it cannot proceed.
                mutation_outcome("t_unloaded_commit", mutation)

                if held:
                    q("SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries")
                    held = False

                # The updating thread finalizes an unknown-state transaction one iteration after
                # loading its entry, and an iteration needs a new entry to start: commit something.
                q("CREATE TABLE t_unloaded_commit_poke (n UInt64) ENGINE = MergeTree ORDER BY n")
                tx(3, "BEGIN TRANSACTION")
                tx(3, "INSERT INTO t_unloaded_commit_poke VALUES (1)")
                tx(3, "COMMIT")

                # The pause justified a postponement of its own, and that reason outlives it.
                require_fresh_mutation_pass("t_unloaded_commit", mutation)

                finished, postponed = mutation_outcome(
                    "t_unloaded_commit", mutation, stop_early=False
                )
                if finished:
                    mutation.result(timeout=FUTURE_TIMEOUT)
                else:
                    require(
                        postponed,
                        "the mutation neither finished nor was postponed for part visibility, so "
                        "this run shows nothing about the defect",
                    )
                    kill_query(mutation_id)
                    require_cancelled(mutation, "the mutation")
                commit.result(timeout=FUTURE_TIMEOUT)

            finally:
                # Release the pause before the pool is drained: its join would otherwise wait for
                # a commit this test is itself holding.
                if held:
                    with contextlib.suppress(Exception):
                        q(
                            "SYSTEM DISABLE FAILPOINT "
                            "transaction_log_pause_before_loading_entries"
                        )
                    held = False
        assert finished, f"the mutation did not finish within {HANG_TIMEOUT} s"

        tx(1, "COMMIT")
        assert q("SELECT n, v FROM t_unloaded_commit ORDER BY n") == "1\t1\n2\t1\n"
    finally:
        with contextlib.suppress(Exception):
            tx(1, "ROLLBACK")
        if held:
            with contextlib.suppress(Exception):
                q("SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries")


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124488: the cleanup thread removes a part that a lowered snapshot still sees",
)
def test_lowered_snapshot_keeps_the_part_it_sees(start_cluster):
    table = "t_snapshot_keeps_part"
    q(f"DROP TABLE IF EXISTS {table} SYNC")
    q(
        f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS "
        "old_parts_lifetime = 3600, cleanup_delay_period = 1, max_cleanup_delay_period = 1, "
        "cleanup_delay_period_random_add = 0"
    )

    def parts_on_disk(timeout=QUERY_TIMEOUT):
        return int(
            q(
                "SELECT count() FROM system.parts WHERE database = currentDatabase() "
                f"AND table = '{table}'",
                timeout=timeout,
            ).strip()
        )

    def last_removal_attempt(timeout=QUERY_TIMEOUT):
        """The newest `last_removal_attempt_time` over the parts of `table`, in epoch seconds.

        Taken as a baseline for `kept_as_visible`, because the setup leaves a stamp of its own:
        `StorageMergeTree::truncate` cleans up synchronously, in a round that ran while the
        truncating transaction still protected the part.
        """
        return int(
            q(
                "SELECT max(toUnixTimestamp(last_removal_attempt_time)) FROM system.parts "
                f"WHERE database = currentDatabase() AND table = '{table}'",
                timeout=timeout,
            ).strip()
            or 0
        )

    def kept_as_visible(since, timeout=QUERY_TIMEOUT):
        """Whether a cleanup round after `since` kept the part because a transaction still sees it.

        `grabOldParts` stamps the attempt time first, then tests visibility, then unique ownership,
        and `removal_state` says which test stopped it. A fresh attempt time on its own would also
        accept a round that found the part removable and skipped it under `NON_UNIQUE_OWNERSHIP` --
        the reference this poll's own read of `system.parts` holds -- which leaves the same part on
        disk as the snapshot protecting it, and would report a server that still removes it as one
        that does not.
        """
        return int(
            q(
                "SELECT countIf(removal_state = 'VISIBLE_TO_TRANSACTIONS' AND "
                f"toUnixTimestamp(last_removal_attempt_time) > {since}) FROM system.parts "
                f"WHERE database = currentDatabase() AND table = '{table}'",
                timeout=timeout,
            ).strip()
            or 0
        )

    # Nothing protects the part between the truncating commit and the lowered snapshot below: the
    # transactional `TRUNCATE` hands it to `removePartsFromWorkingSet` with `clear_without_timeout`,
    # which sets `remove_time = 0`, and `grabOldParts` then ignores `old_parts_lifetime`.
    q(f"SYSTEM STOP CLEANUP {table}")
    try:
        wait_for_no_cleanup(table)

        tx(1, "BEGIN TRANSACTION")
        tx(1, f"INSERT INTO {table} VALUES (1)")
        tx(1, "COMMIT")
        snapshot = int(q("SELECT transactionLatestSnapshot()").strip())

        tx(2, "BEGIN TRANSACTION")
        tx(2, f"TRUNCATE TABLE {table}")
        tx(2, "COMMIT")

        tx(3, "BEGIN TRANSACTION")
        tx(3, f"SET TRANSACTION SNAPSHOT {snapshot}")
        seen = tx(3, f"SELECT count() FROM {table}").strip()
        require(seen == "1", f"the lowered snapshot does not see the row: {seen}")

        q(f"ALTER TABLE {table} MODIFY SETTING old_parts_lifetime = 1")
        attempted = last_removal_attempt()

        def cleanup_had_its_chance(remaining):
            """Lets cleanup run one round, then reads what that finished round decided.

            The attempt time is written at the top of `grabOldParts` and the reason only after the
            test that stopped it, and `system.parts` reads the two columns separately: a read taken
            while a round is in flight can pair this round's attempt time with the reason of the
            round before it. So the read happens with cleanup stopped and its round drained, which
            is also why the chance is given one round at a time.

            A round that found the part removable and skipped it under `NON_UNIQUE_OWNERSHIP` -- the
            reference these reads hold -- is not the chance the defect needs, so the next round has
            to beat the attempt time of this one.
            """
            nonlocal attempted
            q(f"SYSTEM START CLEANUP {table}")
            try:
                poll(
                    lambda left: parts_on_disk(left) == 0 or last_removal_attempt(left) > attempted,
                    min(10, remaining),
                )
            finally:
                q(f"SYSTEM STOP CLEANUP {table}")
                wait_for_no_cleanup(table)
            if parts_on_disk() == 0:
                return True
            kept = kept_as_visible(attempted)
            attempted = last_removal_attempt()
            return kept

        # Stop early once the part is gone: that is the defect the assertions below report.
        require(
            poll(cleanup_had_its_chance, 60, step=0),
            "no cleanup round since the snapshot was lowered got as far as deciding that the part "
            "is still visible to a transaction, so this run says nothing about what it skips",
        )

        assert tx(3, f"SELECT count() FROM {table}").strip() == "1"
        assert parts_on_disk() == 1, "the cleanup removed a part the transaction still sees"
    finally:
        q(f"SYSTEM START CLEANUP {table}")
        # The setup can fail before this session has a transaction; teardown reports a rollback
        # that is genuinely stuck.
        with contextlib.suppress(Exception):
            tx(3, "ROLLBACK")

    # Once no transaction can see it, the part goes.
    require(
        poll(lambda remaining: parts_on_disk(remaining) == 0, 60),
        "the part was never cleaned up, so this run says nothing about what cleanup skipped",
    )


# https://github.com/ClickHouse/ClickHouse/issues/124486
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124486: a failed non-transactional DROP PARTITION leaves a part stamped "
    "for removal, which hides it from every transaction. #122375 repaired the reload, not the "
    "stamp",
)
def test_failed_drop_partition_leaves_no_removal_stamp(start_cluster):
    two_parts("t_drop_stamp")
    with merges_and_cleanup_stopped():
        require_parts("t_drop_stamp", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = q_error("ALTER TABLE t_drop_stamp DROP PARTITION tuple()")
        require_injected_failure(error)

        stamped = q(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            "AND table = 't_drop_stamp' AND removal_csn != 0"
        ).strip()
        assert stamped == "0", "the failed statement left a part stamped for removal"

        tx(1, "BEGIN TRANSACTION")
        visible = tx(1, "SELECT count(), sum(n) FROM t_drop_stamp").strip()
        tx(1, "ROLLBACK")
        assert visible == "2\t3", f"a transaction cannot see the rows that survived: {visible}"

        # The failure left no lock behind: the same statement succeeds now.
        q("ALTER TABLE t_drop_stamp DROP PARTITION tuple()")
        remaining = q("SELECT count() FROM t_drop_stamp").strip()
        require(remaining == "0", f"the retried drop left {remaining} rows")


# https://github.com/ClickHouse/ClickHouse/issues/124486
# https://github.com/ClickHouse/ClickHouse/pull/122375
def test_failed_drop_partition_survives_reload(start_cluster):
    """A failed `DROP PARTITION` keeps the partition across a reload.

    The rollback marks `creation_csn = RolledBackCSN` in memory only, so the empty covering parts
    have to leave the disk inside the failing statement. Merges and cleanup are stopped: a
    directory that is still there when the assertion runs is one the statement left behind, and
    the loader reads it as prehistoric covering the originals.
    """
    two_parts("t_drop_reload")
    with merges_and_cleanup_stopped():
        require_parts("t_drop_reload", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = q_error("ALTER TABLE t_drop_reload DROP PARTITION tuple()")
        require_injected_failure(error)
        require_rows("t_drop_reload", "2\t3")

        left = leftover_part_dirs("t_drop_reload")
        assert left == [], f"the failed drop left its empty covering parts on disk: {left}"
        logging.info("before the reload: %s", reload_state("t_drop_reload"))
        q("DETACH TABLE t_drop_reload")
        q("ATTACH TABLE t_drop_reload")
        logging.info("after the reload: %s", reload_state("t_drop_reload"))
        assert q("SELECT count(), sum(n) FROM t_drop_reload").strip() == "2\t3"


# https://github.com/ClickHouse/ClickHouse/issues/124486
# https://github.com/ClickHouse/ClickHouse/pull/122375
def test_failed_truncate_survives_reload(start_cluster):
    """A failed `TRUNCATE` keeps the rows across a reload.

    `TRUNCATE` takes the same path as `DROP PARTITION`; see
    `test_failed_drop_partition_survives_reload` for what the assertions turn on.
    """
    two_parts("t_truncate_reload")
    with merges_and_cleanup_stopped():
        require_parts("t_truncate_reload", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = q_error("TRUNCATE TABLE t_truncate_reload")
        require_injected_failure(error)
        require_rows("t_truncate_reload", "2\t3")

        left = leftover_part_dirs("t_truncate_reload")
        assert left == [], f"the failed truncate left its empty covering parts on disk: {left}"
        logging.info("before the reload: %s", reload_state("t_truncate_reload"))
        q("DETACH TABLE t_truncate_reload")
        q("ATTACH TABLE t_truncate_reload")
        logging.info("after the reload: %s", reload_state("t_truncate_reload"))
        assert q("SELECT count(), sum(n) FROM t_truncate_reload").strip() == "2\t3"


# https://github.com/ClickHouse/ClickHouse/issues/124486
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124486: a failed REPLACE PARTITION leaves the new part beside the ones it "
    "did not replace. `replacePartitionFrom` is not the path #122375 repaired",
)
def test_failed_replace_partition_leaves_destination_intact(start_cluster):
    q("DROP TABLE IF EXISTS t_replace_src SYNC")
    q("DROP TABLE IF EXISTS t_replace_dst SYNC")
    q("CREATE TABLE t_replace_src (n UInt64) ENGINE = MergeTree ORDER BY n")
    q("CREATE TABLE t_replace_dst (n UInt64) ENGINE = MergeTree ORDER BY n")
    # See `two_parts`: the destination's two parts must survive as two.
    q("SYSTEM STOP MERGES t_replace_dst")
    q("INSERT INTO t_replace_src VALUES (100)")
    q("INSERT INTO t_replace_dst VALUES (1)")
    q("INSERT INTO t_replace_dst VALUES (2)")

    with merges_and_cleanup_stopped():
        require_parts("t_replace_dst", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = q_error(
                "ALTER TABLE t_replace_dst REPLACE PARTITION tuple() FROM t_replace_src"
            )
        require_injected_failure(error)

        kept = q("SELECT arraySort(groupArray(n)) FROM t_replace_dst").strip()
        assert kept == "[1,2]", f"the destination changed after a failed replace: {kept}"

        # The failure left no lock behind: the same statement succeeds now.
        q("ALTER TABLE t_replace_dst REPLACE PARTITION tuple() FROM t_replace_src")
        replaced = q("SELECT arraySort(groupArray(n)) FROM t_replace_dst").strip()
        require(replaced == "[100]", f"the retried replace left {replaced}")


# https://github.com/ClickHouse/ClickHouse/issues/124486
# https://github.com/ClickHouse/ClickHouse/pull/122375
@pytest.mark.skip(
    reason="Kills the node for the rest of the module. When the rollback itself throws, the "
    "handler added by #122375 hands a part in state `Temporary` to `tryRemovePartImmediately`, "
    "which raises `Cannot remove part ..., because it has state: Temporary`; a debug or sanitizer "
    "build aborts on it. Drop the marker once that is fixed - the reload below still loses the "
    "rows, because the part the handler could not remove keeps its final name on disk",
)
def test_failed_drop_partition_survives_reload_when_rollback_mark_fails(start_cluster):
    two_parts("t_rollback_mark")
    with merges_and_cleanup_stopped():
        require_parts("t_rollback_mark", 2)
        marks_failed = log_hits(rollback_mark_log("t_rollback_mark"))
        with failpoints(
            "non_transactional_removal_store_fail_after_first_part",
            "version_metadata_store_creation_csn_fail",
        ):
            error = q_error(
                "ALTER TABLE t_rollback_mark DROP PARTITION tuple()",
                settings={"send_logs_level": "none"},
            )
        require_injected_failure(error)
        require_rollback_mark_failed("t_rollback_mark", marks_failed)
        require_rows("t_rollback_mark", "2\t3")

        require_rolled_back_parts_on_disk("t_rollback_mark")
        logging.info("before the reload: %s", reload_state("t_rollback_mark"))
        q("DETACH TABLE t_rollback_mark")
        q("ATTACH TABLE t_rollback_mark")
        logging.info("after the reload: %s", reload_state("t_rollback_mark"))
        assert q("SELECT count(), sum(n) FROM t_rollback_mark").strip() == "2\t3"


# https://github.com/ClickHouse/ClickHouse/issues/124489
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124489: a DROP PARTITION racing a transaction that removes one of its "
    "parts is accepted, and the part is active beside the empty part covering it",
)
def test_drop_partition_racing_a_removal_is_refused(start_cluster):
    table = "t_drop_locked"
    q(f"DROP TABLE IF EXISTS {table} SYNC")
    q(
        f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n "
        "SETTINGS remove_empty_parts = 0"
    )
    q(f"SYSTEM STOP MERGES {table}")
    try:
        for n in (1, 2, 3):
            q(f"INSERT INTO {table} VALUES ({n})")

        failpoint = "non_transactional_drop_pause_before_publish"
        try:
            with workers(1) as pool:
                with failpoints(failpoint):
                    drop = pool.submit(
                        functools.partial(
                            node.query_and_get_answer_with_error,
                            f"ALTER TABLE {table} DROP PARTITION tuple()",
                            timeout=QUERY_TIMEOUT,
                        )
                    )
                    q(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)

                    tx(1, "BEGIN TRANSACTION")
                    tx(1, f"ALTER TABLE {table} DROP PART 'all_2_2_0'")

                    q(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
                    _, error = drop.result(timeout=FUTURE_TIMEOUT)

            tx(1, "ROLLBACK")

            require(
                not error or "SERIALIZATION_ERROR" in error,
                f"the racing drop failed for an unrelated reason: {error}",
            )
            assert error, "the drop that raced with the removal was accepted"
            assert covered_active_parts(table) == [], (
                "a part is active beside a part that covers it"
            )
        finally:
            with contextlib.suppress(Exception):
                tx(1, "ROLLBACK")
    finally:
        q(f"SYSTEM START MERGES {table}")


# https://github.com/ClickHouse/ClickHouse/issues/124487
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124487: an acknowledged INSERT is lost when the server stops between the "
    "sync and the rename of the first version metadata store",
)
def test_acknowledged_insert_survives_kill_during_first_version_metadata_store(
    start_cluster,
):
    # A part inserted outside a transaction has no `txn_version.txt`. The first metadata store for
    # it is the removal lock taken by a transaction's DROP PARTITION. The store writes
    # `txn_version.txt.tmp` and renames it; a kill between the two must not lose the rows.
    q("DROP TABLE IF EXISTS mt_lost SYNC")
    q(
        "CREATE TABLE mt_lost (n UInt64) ENGINE = MergeTree ORDER BY n PARTITION BY n % 2 "
        "SETTINGS remove_empty_parts = 0"
    )
    q("INSERT INTO mt_lost VALUES (1), (3)")
    part_path = q(
        "SELECT path FROM system.parts WHERE database = currentDatabase() "
        "AND table = 'mt_lost' AND active"
    ).strip()
    require(part_path != "", "the insert produced no active part")

    failpoint = "version_metadata_store_pause_before_rename"
    with workers(1) as pool:
        with failpoints(failpoint):
            tx(7, "BEGIN TRANSACTION")
            drop = pool.submit(
                in_session_answer_with_error(7, "ALTER TABLE mt_lost DROP PARTITION ID '1'")
            )
            q(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)

            listing = node.exec_in_container(["bash", "-c", f"ls -1 {part_path}"]).split()
            require(
                "txn_version.txt.tmp" in listing and "txn_version.txt" not in listing,
                f"the pause did not leave a torn first store behind: {listing}",
            )

            restart_server()
            with contextlib.suppress(Exception):
                drop.result(timeout=FUTURE_TIMEOUT)

    q("SYSTEM WAIT LOADING PARTS mt_lost")
    assert q("SELECT n FROM mt_lost ORDER BY n").strip() == "1\n3"
    assert (
        q(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            "AND table = 'mt_lost' AND active"
        ).strip()
        == "1"
    )


# Last in the file: today's `LOGICAL_ERROR` aborts a debug or sanitizer build, and the tests above
# need a server that is still running.
# https://github.com/ClickHouse/ClickHouse/issues/124489
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known bug #124489: removing a part whose creating transaction is still running raises "
    "LOGICAL_ERROR instead of SERIALIZATION_ERROR, which aborts a debug or sanitizer build",
)
def test_drop_partition_of_uncommitted_creation_is_refused(start_cluster):
    table = "t_uncommitted_drop"
    try:
        q(f"DROP TABLE IF EXISTS {table} SYNC")
        q(f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n PARTITION BY n % 2")
        q(f"INSERT INTO {table} VALUES (1)")

        tx(1, "BEGIN TRANSACTION")
        tx(1, f"INSERT INTO {table} VALUES (2)")

        tx(2, "BEGIN TRANSACTION")
        tx(2, "SET TRANSACTION SNAPSHOT 3")
        seen = tx(2, f"SELECT count() FROM {table}").strip()
        require(seen == "2", f"the dropping transaction does not see both parts: {seen}")

        creation_csn_lines = log_hits(CREATION_CSN_LOG)
        creation_csn_stderr = log_hits(CREATION_CSN_LOG, filename="stderr.log")
        try:
            _, error = tx_answer_with_error(2, f"ALTER TABLE {table} DROP PARTITION 0")
        except Exception as e:
            # A debug or sanitizer build aborts on the `LOGICAL_ERROR`, so the request never
            # answers. That is the same defect reported through a dead connection -- but a lost
            # connection says nothing on its own: the process has to be gone, and the log has to
            # name this exception. A failed `SELECT 1` is not evidence of either.
            # The exception is logged before `abort`, and fatal reporting can then run for
            # minutes (`SignalHandlers.cpp`), so the log line arrives long before the process
            # does. Waiting on the process first would call the real abort an unrelated failure.
            require(
                poll(
                    lambda _: log_hits(CREATION_CSN_LOG) > creation_csn_lines
                    or log_hits(CREATION_CSN_LOG, filename="stderr.log") > creation_csn_stderr,
                    120,
                ),
                f"the request failed without the creation-CSN exception being reported: {e}",
            )
            error = f"the server aborted on the creation-CSN exception: {e}"
        require(
            not error
            or "SERIALIZATION_ERROR" in error
            or CREATION_CSN_LOG in error
            or "aborted on the creation-CSN exception" in error,
            f"the drop failed for an unrelated reason: {error}",
        )
        assert error and "SERIALIZATION_ERROR" in error, (
            f"the drop was not refused with SERIALIZATION_ERROR: {error!r}"
        )
        tx(2, "ROLLBACK")

        # The creating transaction is unaffected.
        tx(1, "COMMIT")
        committed = q(f"SELECT n FROM {table} ORDER BY n").strip()
        require(committed == "1\n2", f"the creating transaction lost its row: {committed}")

        # A transaction may drop a part it created itself.
        tx(3, "BEGIN TRANSACTION")
        tx(3, f"INSERT INTO {table} VALUES (4)")
        tx(3, f"ALTER TABLE {table} DROP PARTITION 0")
        tx(3, "COMMIT")
        left = q(f"SELECT n FROM {table} ORDER BY n").strip()
        require(left == "1", f"dropping a self-created part left {left!r}")
    finally:
        restart_if_down()

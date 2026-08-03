"""
Tests for the CIDB log-export sender snapshot in
`ci.jobs.scripts.clickhouse_proc.ClickHouseProc`.

The CIDB-staging-overload heuristic in `FTResultsProcessor` treats
`system.<table>_sender.DistributedInsertQueue.*` errors as CI infrastructure
noise, so the set of sender tables it is keyed off must be an identity a test
cannot produce. A name alone is not: tests can create and drop tables in the
`system` database (e.g.
`tests/queries/0_stateless/02494_query_cache_system_tables.sql`), so a test
could drop a CI-created sender and recreate its own `Distributed` table under
the same name. The `uuid` alone is not enough either: `system` is an `Atomic`
database in the server, and `CREATE TABLE ... UUID '<captured uuid>'` is
accepted there, so a test can also reuse the captured `uuid`.

The snapshot therefore fingerprints each sender by `uuid`,
`metadata_modification_time` and a hash of `engine_full`, and
`verify_log_export_senders` re-reads `system.tables` after the suite and keeps
only the names whose fingerprint is unchanged. The metadata timestamp is the
part no SQL can choose: a table recreated during the suite is stamped later
than the pre-suite snapshot.

See the `clickhouse-gh[bot]` reviews on ClickHouse/ClickHouse#106176.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.praktika  # noqa: F401

from ci.jobs.scripts import clickhouse_proc as chp

_QUERY_LOG_UUID = "11111111-1111-1111-1111-111111111111"
_TRACE_LOG_UUID = "22222222-2222-2222-2222-222222222222"
_ZERO_UUID = "00000000-0000-0000-0000-000000000000"
_ENGINE_HASH = "A1B2C3D4E5F60718"
_SETUP_TIME = "2026-08-03 07:00:00"
_MID_SUITE_TIME = "2026-08-03 07:42:11"


def _proc():
    return chp.ClickHouseProc.__new__(chp.ClickHouseProc)


def _row(name, uuid, mtime=_SETUP_TIME, engine_hash=_ENGINE_HASH):
    return f"{name}\t{uuid}\t{mtime}\t{engine_hash}\n"


def _fake_output(monkeypatch, text):
    monkeypatch.setattr(chp.Shell, "get_output", staticmethod(lambda *a, **kw: text))


def _fingerprint(uuid, mtime=_SETUP_TIME, engine_hash=_ENGINE_HASH):
    return f"{uuid}|{mtime}|{engine_hash}"


_SNAPSHOT = {
    "query_log_sender": _fingerprint(_QUERY_LOG_UUID),
    "trace_log_sender": _fingerprint(_TRACE_LOG_UUID),
}


def test_snapshot_captures_the_full_fingerprint(monkeypatch):
    _fake_output(
        monkeypatch,
        _row("query_log_sender", _QUERY_LOG_UUID) + _row("trace_log_sender", _TRACE_LOG_UUID),
    )
    assert _proc().get_log_export_senders() == _SNAPSHOT


def test_snapshot_is_empty_when_the_query_fails(monkeypatch):
    _fake_output(monkeypatch, "")
    assert _proc().get_log_export_senders() == {}


def test_snapshot_drops_tables_without_a_real_uuid(monkeypatch):
    """A zero `uuid` (a non-`Atomic` `system` database) carries no identity to
    verify later, so such a table must not enter the snapshot at all."""
    _fake_output(
        monkeypatch,
        _row("query_log_sender", _ZERO_UUID) + _row("trace_log_sender", _TRACE_LOG_UUID),
    )
    assert _proc().get_log_export_senders() == {
        "trace_log_sender": _fingerprint(_TRACE_LOG_UUID)
    }


def test_snapshot_drops_tables_without_a_metadata_timestamp(monkeypatch):
    """An unset `metadata_modification_time` would let a recreated table match
    the snapshot, so it is required as well."""
    _fake_output(
        monkeypatch,
        _row("query_log_sender", _QUERY_LOG_UUID, mtime="1970-01-01 00:00:00")
        + _row("trace_log_sender", _TRACE_LOG_UUID),
    )
    assert _proc().get_log_export_senders() == {
        "trace_log_sender": _fingerprint(_TRACE_LOG_UUID)
    }


def test_verify_keeps_unchanged_senders(monkeypatch):
    _fake_output(
        monkeypatch,
        _row("query_log_sender", _QUERY_LOG_UUID) + _row("trace_log_sender", _TRACE_LOG_UUID),
    )
    assert _proc().verify_log_export_senders(_SNAPSHOT) == {
        "query_log_sender",
        "trace_log_sender",
    }


def test_verify_drops_a_sender_rebound_under_the_same_name(monkeypatch):
    """A test drops `system.query_log_sender` and creates its own
    `Distributed` table under that name. The name is unchanged but the `uuid`
    is not, so the heuristic must stop counting it."""
    _fake_output(
        monkeypatch,
        _row("query_log_sender", "33333333-3333-3333-3333-333333333333", _MID_SUITE_TIME)
        + _row("trace_log_sender", _TRACE_LOG_UUID),
    )
    assert _proc().verify_log_export_senders(_SNAPSHOT) == {"trace_log_sender"}


def test_verify_drops_a_sender_recreated_with_the_captured_uuid(monkeypatch):
    """`CREATE TABLE ... UUID '<captured uuid>'` is accepted in an `Atomic`
    database, so a test can reuse the captured `uuid`. The recreated table is
    still stamped with a new `metadata_modification_time`, which no SQL can
    backdate, so it must drop out."""
    _fake_output(
        monkeypatch,
        _row("query_log_sender", _QUERY_LOG_UUID, _MID_SUITE_TIME)
        + _row("trace_log_sender", _TRACE_LOG_UUID),
    )
    assert _proc().verify_log_export_senders(_SNAPSHOT) == {"trace_log_sender"}


def test_verify_drops_a_sender_recreated_with_a_different_engine(monkeypatch):
    """The last component: same name, same `uuid`, same metadata timestamp,
    but a different engine definition than the one log export created."""
    _fake_output(
        monkeypatch,
        _row("query_log_sender", _QUERY_LOG_UUID, engine_hash="0000000000000000")
        + _row("trace_log_sender", _TRACE_LOG_UUID),
    )
    assert _proc().verify_log_export_senders(_SNAPSHOT) == {"trace_log_sender"}


def test_verify_drops_a_sender_that_disappeared(monkeypatch):
    _fake_output(monkeypatch, _row("trace_log_sender", _TRACE_LOG_UUID))
    assert _proc().verify_log_export_senders(_SNAPSHOT) == {"trace_log_sender"}


def test_verify_fails_closed_when_the_server_is_gone(monkeypatch):
    """No output at all - the server really did die, or the client failed.
    Every name drops out, the classifier abstains, and the run keeps its
    `Server died` verdict."""
    _fake_output(monkeypatch, "")
    assert _proc().verify_log_export_senders(_SNAPSHOT) == set()


def test_rebound_sender_keeps_server_died_end_to_end(tmp_path, monkeypatch):
    """The full path the heuristic runs through: a test rebinds
    `system.query_log_sender` (same `uuid`, new metadata timestamp) and floods
    the server log with shipping errors from it, then the runner is killed by
    the wall-clock timeout. The verified sender set no longer contains that
    name, so `FTResultsProcessor` must keep the `Server died` `FAIL` instead of
    greening the run as a CIDB outage."""
    import signal

    from ci.jobs.scripts.functional_tests_results import (
        FTResultsProcessor,
        _STAGING_OVERLOAD_MIN_ERRORS,
    )
    from ci.praktika.result import Result

    _fake_output(
        monkeypatch,
        _row("query_log_sender", _QUERY_LOG_UUID, _MID_SUITE_TIME),
    )
    verified = _proc().verify_log_export_senders(
        {"query_log_sender": _fingerprint(_QUERY_LOG_UUID)}
    )
    assert verified == set()

    shipping_error = (
        "2026.08.03 07:43:00.000000 [ 4242 ] {} <Error> "
        "system.query_log_sender.DistributedInsertQueue.default: Failed to send "
        "batch due to: Code: 210. DB::NetException: Connection refused "
        "(NETWORK_ERROR) some-test-shard:9000"
    )
    err_log = tmp_path / "clickhouse-server.err.log"
    err_log.write_text(
        "".join(f"{shipping_error}\n" for _ in range(_STAGING_OVERLOAD_MIN_ERRORS * 2)),
        encoding="utf-8",
    )
    (tmp_path / "test_result.txt").write_text(
        "00001_some_test: [ OK ] 1.00 sec.\nAll tests have finished\n", encoding="utf-8"
    )

    result = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(err_log),
        log_export_senders=verified,
    ).run(runner_exit_code=-signal.SIGTERM)

    assert result.status == Result.Status.FAIL
    leaf_names = [r.name for r in result.results]
    assert "Server died" in leaf_names
    assert "CIDB log cluster unresponsive" not in leaf_names


def test_verify_of_an_empty_snapshot_does_not_query(monkeypatch):
    """A run that never started log export (or a swapped bugfix-validation
    binary) has nothing to verify and must not depend on the server at all."""

    def _boom(*_a, **_kw):
        raise AssertionError("verify_log_export_senders queried an empty snapshot")

    monkeypatch.setattr(chp.Shell, "get_output", staticmethod(_boom))
    assert _proc().verify_log_export_senders({}) == set()

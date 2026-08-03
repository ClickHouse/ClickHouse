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
the same name.

`system` is an `Atomic` database in the server, so the recreated table gets a
fresh `uuid`. The snapshot therefore records `{name: uuid}` before any test
runs, and `verify_log_export_senders` re-reads `system.tables` after the suite
and keeps only the names whose `uuid` is unchanged.

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


def _proc():
    return chp.ClickHouseProc.__new__(chp.ClickHouseProc)


def _fake_output(monkeypatch, text):
    monkeypatch.setattr(chp.Shell, "get_output", staticmethod(lambda *a, **kw: text))


def test_snapshot_captures_name_and_uuid(monkeypatch):
    _fake_output(
        monkeypatch,
        f"query_log_sender\t{_QUERY_LOG_UUID}\ntrace_log_sender\t{_TRACE_LOG_UUID}\n",
    )
    assert _proc().get_log_export_senders() == {
        "query_log_sender": _QUERY_LOG_UUID,
        "trace_log_sender": _TRACE_LOG_UUID,
    }


def test_snapshot_is_empty_when_the_query_fails(monkeypatch):
    _fake_output(monkeypatch, "")
    assert _proc().get_log_export_senders() == {}


def test_snapshot_drops_tables_without_a_real_uuid(monkeypatch):
    """A zero `uuid` (a non-`Atomic` `system` database) carries no identity to
    verify later, so such a table must not enter the snapshot at all."""
    _fake_output(
        monkeypatch, f"query_log_sender\t{_ZERO_UUID}\ntrace_log_sender\t{_TRACE_LOG_UUID}\n"
    )
    assert _proc().get_log_export_senders() == {"trace_log_sender": _TRACE_LOG_UUID}


def test_verify_keeps_unchanged_senders(monkeypatch):
    snapshot = {
        "query_log_sender": _QUERY_LOG_UUID,
        "trace_log_sender": _TRACE_LOG_UUID,
    }
    _fake_output(
        monkeypatch,
        f"query_log_sender\t{_QUERY_LOG_UUID}\ntrace_log_sender\t{_TRACE_LOG_UUID}\n",
    )
    assert _proc().verify_log_export_senders(snapshot) == {
        "query_log_sender",
        "trace_log_sender",
    }


def test_verify_drops_a_sender_rebound_under_the_same_name(monkeypatch):
    """The bot's scenario: a test drops `system.query_log_sender` and creates
    its own `Distributed` table under that name. The name is unchanged but the
    `uuid` is not, so the heuristic must stop counting it."""
    snapshot = {
        "query_log_sender": _QUERY_LOG_UUID,
        "trace_log_sender": _TRACE_LOG_UUID,
    }
    _fake_output(
        monkeypatch,
        "query_log_sender\t33333333-3333-3333-3333-333333333333\n"
        f"trace_log_sender\t{_TRACE_LOG_UUID}\n",
    )
    assert _proc().verify_log_export_senders(snapshot) == {"trace_log_sender"}


def test_verify_drops_a_sender_that_disappeared(monkeypatch):
    snapshot = {
        "query_log_sender": _QUERY_LOG_UUID,
        "trace_log_sender": _TRACE_LOG_UUID,
    }
    _fake_output(monkeypatch, f"trace_log_sender\t{_TRACE_LOG_UUID}\n")
    assert _proc().verify_log_export_senders(snapshot) == {"trace_log_sender"}


def test_verify_fails_closed_when_the_server_is_gone(monkeypatch):
    """No output at all - the server really did die, or the client failed.
    Every name drops out, the classifier abstains, and the run keeps its
    `Server died` verdict."""
    snapshot = {"query_log_sender": _QUERY_LOG_UUID}
    _fake_output(monkeypatch, "")
    assert _proc().verify_log_export_senders(snapshot) == set()


def test_verify_of_an_empty_snapshot_does_not_query(monkeypatch):
    """A run that never started log export (or a swapped bugfix-validation
    binary) has nothing to verify and must not depend on the server at all."""

    def _boom(*_a, **_kw):
        raise AssertionError("verify_log_export_senders queried an empty snapshot")

    monkeypatch.setattr(chp.Shell, "get_output", staticmethod(_boom))
    assert _proc().verify_log_export_senders({}) == set()

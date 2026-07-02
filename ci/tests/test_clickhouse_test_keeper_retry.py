"""
Regression test for transient-Keeper-error retrying in tests/clickhouse-test.

The functional test runner retries a test when its output contains one of the
substrings in MESSAGES_TO_RETRY. Transient Keeper errors render in two forms:

  * socket-layer (Coordination::Exception::fromMessage), e.g.
        Code: 999. Coordination::Exception: Session expired. (KEEPER_EXCEPTION)
  * generic operation-level (default / fromPath ctor), which prepends
    "Coordination error: ", e.g.
        Code: 999. Coordination::Exception: Coordination error: Connection loss, path /x. (KEEPER_EXCEPTION)

The retry list originally carried only the socket-layer form, so the generic
form (the majority of throw sites in src/Common/ZooKeeper/ZooKeeper.cpp) was
never matched and a transient Keeper error turned into a hard FAIL instead of
being retried. This test loads the real runner module and asserts need_retry()
returns True for every rendered form.
"""

import importlib.machinery
import importlib.util
import os
import types

_RUNNER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "tests", "clickhouse-test"
)


def _load_runner():
    # tests/clickhouse-test has no .py extension; load it explicitly. All of its
    # executable code is guarded by `if __name__ == "__main__"`, so import is a no-op.
    loader = importlib.machinery.SourceFileLoader(
        "clickhouse_test_runner", _RUNNER_PATH
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


_RUNNER = _load_runner()

# stderr lines exactly as the client renders them (Code/displayText/error-name).
_SOCKET_LAYER = [
    "Code: 999. Coordination::Exception: Connection loss. (KEEPER_EXCEPTION)",
    "Code: 999. Coordination::Exception: Session expired. (KEEPER_EXCEPTION)",
]
_GENERIC = [
    "Code: 999. Coordination::Exception: Coordination error: Connection loss, "
    "path /clickhouse/tables/x/log. (KEEPER_EXCEPTION)",
    "Code: 999. Coordination::Exception: Coordination error: Session expired, "
    "path /clickhouse/tables/x/log. (KEEPER_EXCEPTION)",
    "Code: 999. Coordination::Exception: Coordination error: Connection loss. "
    "(KEEPER_EXCEPTION)",
]


def _need_retry(stderr):
    # check_zookeeper_session=False forces the substring scan over MESSAGES_TO_RETRY
    # (the path that was buggy) instead of the ZooKeeper-session-uptime shortcut.
    args = types.SimpleNamespace(check_zookeeper_session=False)
    return _RUNNER.need_retry(args, "", stderr, 0)


def test_socket_layer_keeper_errors_are_retried():
    for stderr in _SOCKET_LAYER:
        assert _need_retry(stderr), stderr


def test_generic_operation_level_keeper_errors_are_retried():
    # These render with the "Coordination error: " infix; missed before the fix.
    for stderr in _GENERIC:
        assert _need_retry(stderr), stderr


def test_non_keeper_error_is_not_retried():
    assert not _need_retry(
        "Code: 47. DB::Exception: Unknown identifier: x. (UNKNOWN_IDENTIFIER)"
    )

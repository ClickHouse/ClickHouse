#!/usr/bin/env bash
# The functional test runner (tests/clickhouse-test) retries a test whose output
# matches MESSAGES_TO_RETRY. Coordination::Exception has two renderings depending
# on the constructor: the socket layer prints "Coordination::Exception: Session expired"
# while generic Keeper operations (exists/get/getChildren) print
# "Coordination::Exception: Coordination error: Session expired". This test loads the
# runner as a module and checks that `need_retry` accepts both renderings and still
# rejects unrelated output.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RUNNER="$CUR_DIR/../../clickhouse-test"

python3 - "$RUNNER" <<'PY'
import contextlib
import importlib.machinery
import importlib.util
import io
import sys
import types

runner_path = sys.argv[1]
loader = importlib.machinery.SourceFileLoader("clickhouse_test_runner", runner_path)
spec = importlib.util.spec_from_loader("clickhouse_test_runner", loader)
runner = importlib.util.module_from_spec(spec)
# Module-level code of the runner may print warnings (e.g. about missing jinja2);
# they are not part of what this test checks.
with contextlib.redirect_stdout(io.StringIO()):
    loader.exec_module(runner)

# Without a Keeper session to inspect, `need_retry` reduces to the MESSAGES_TO_RETRY match.
args = types.SimpleNamespace(check_zookeeper_session=False)

def check(name, stdout, stderr, expected):
    got = runner.need_retry(args, stdout, stderr, 0)
    print(f"{name}: {'retry' if got else 'no retry'} {'OK' if got == expected else 'FAIL'}")

socket_expired = "Code: 999. Coordination::Exception: Session expired. (KEEPER_EXCEPTION)"
socket_loss = "Code: 999. Coordination::Exception: Connection loss. (KEEPER_EXCEPTION)"
generic_expired = (
    "Code: 999. Coordination::Exception: Coordination error: Session expired, "
    "path /zookeeper/tables/01/replicas/1/is_active. (KEEPER_EXCEPTION)"
)
generic_loss = (
    "Code: 999. Coordination::Exception: Coordination error: Connection loss, "
    "path /zookeeper/tables/01/log. (KEEPER_EXCEPTION)"
)

check("socket-layer session expired in stdout", socket_expired, "", True)
check("socket-layer connection loss in stderr", "", socket_loss, True)
check("generic session expired in stdout", generic_expired, "", True)
check("generic connection loss in stderr", "", generic_loss, True)
check("generic session expired amid other output", "1\n2\n" + generic_expired + "\n3\n", "", True)
check("unrelated Keeper error", "Code: 999. Coordination::Exception: Coordination error: No node, path /zookeeper/tables/01. (KEEPER_EXCEPTION)", "", False)
check("clean output", "1\n2\n3\n", "", False)
PY

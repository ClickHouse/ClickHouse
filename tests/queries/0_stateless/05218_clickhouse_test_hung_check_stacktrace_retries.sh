#!/usr/bin/env bash
# The hung check's stacktrace dump runs under a caller that kills the whole
# process group when its own wait expires (stress.py waits hung_check.wait
# seconds), so the dump's worst case, socket timeout times attempts, has to fit
# inside that budget or the diagnostic branch never gets to print what it saw.
# This test loads the runner as a module, records the arguments
# get_processlist_with_stacktraces passes to the executor, and pins the timeout
# and the attempt count on all three of its branches. The cluster name is
# printed as well so that three lines produced by one branch cannot pass.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RUNNER="$CUR_DIR/../../clickhouse-test"

python3 - "$RUNNER" <<'PY'
import contextlib
import importlib.machinery
import importlib.util
import inspect
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

# What bounds the dump is the budget that takes effect, so a branch which stops
# passing an argument is reported with the default it falls back to instead of
# aborting the test.
executor_defaults = {
    name: parameter.default
    for name, parameter in inspect.signature(runner.clickhouse_execute).parameters.items()
    if parameter.default is not inspect.Parameter.empty
}

recorded = {}


def recorder(_args, query, **kwargs):
    recorded["query"] = query
    recorded["kwargs"] = kwargs
    return b""


def effective(key):
    return recorded["kwargs"].get(key, executor_defaults[key])


# get_processlist_with_stacktraces resolves the executor as a module global.
runner.clickhouse_execute = recorder

modes = (
    ("default", False, False),
    ("replicated_database", True, False),
    ("shared_catalog", False, True),
)

for label, replicated_database, shared_catalog in modes:
    recorded.clear()
    runner.get_processlist_with_stacktraces(
        types.SimpleNamespace(
            replicated_database=replicated_database, shared_catalog=shared_catalog
        )
    )
    cluster = "none"
    for name in ("test_cluster_database_replicated", "test_cluster_shared_catalog"):
        if name in recorded["query"]:
            cluster = name
    print(
        f"{label}: cluster={cluster}"
        f" timeout={effective('timeout')}"
        f" attempts={effective('max_http_retries')}"
    )
PY

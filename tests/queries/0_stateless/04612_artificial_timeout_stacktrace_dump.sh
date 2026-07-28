#!/usr/bin/env bash

# NOTE: This test is intentionally broken — it never finishes. It exists only to
# exercise the stacktrace-dump reporting fixed in
# https://github.com/ClickHouse/ClickHouse/pull/107173 on the macOS
# `Fast test (arm_darwin)` run: when a test times out and the end-of-run hung
# check fires, the runner must write the full server stacktraces to
# `sql_stacktraces.log` / `c_stacktraces.log` and attach them to the report
# instead of flooding `job.log`. Do NOT merge this test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hang ONLY on macOS. This test exists to exercise the stacktrace dump on the
# `Fast test (arm_darwin)` run; there is no point hanging it on Linux, where the
# on-demand `lldb` install cannot work in the unprivileged fast-test container
# anyway (see `_ensure_lldb_installed` in `tests/clickhouse-test`). Worse, this
# is a plain stateless test, so on Linux it would also run in the Linux
# `Fast test` and every `Stateless tests` job and time them all out. Those jobs
# are transitive `needs` of `Fast test (arm_darwin)` (via the `arm_darwin` build
# and `CORE_BLOCKING_JOB_NAMES`), so their failure trips the per-job `if:` guard
# (`!contains(needs.*.outputs.pipeline_status, 'failure')`) and the macOS job we
# actually care about is skipped before it can run. Exiting cleanly on Linux
# (the reference is empty) keeps that chain green so the macOS job runs.
[ "$(uname)" != "Darwin" ] && exit 0

# Start a long-running server-side query in its own session so that
# `clickhouse-test`'s per-test process-group kill cannot reach it. The query
# keeps running in `system.processes` well past the end of the run, so the
# end-of-run hung check fires and dumps/attaches the server stacktraces.
# `os.setsid` is used instead of `setsid(1)`, which is absent on macOS; running
# it in the backgrounded child (not a process-group leader) lets `setsid`
# succeed. The client command is passed as argv words, so no nested quoting.
#
# `max_execution_time = 0` and `max_estimated_execution_time = 0` disable the
# server-side execution-time guard: with the fast-test profile default of 600s,
# the speed estimator otherwise aborts the query (`Code: 160 ... Estimated query
# execution time is too long`) after a few minutes — before the end-of-run hung
# check runs — so `system.processes` would be empty and nothing would be dumped.
python3 -c 'import os, sys, subprocess; os.setsid(); subprocess.Popen(sys.argv[1:])' \
    $CLICKHOUSE_CLIENT --query "SELECT sleepEachRow(1) FROM numbers(100000) SETTINGS max_block_size = 1, max_execution_time = 0, max_estimated_execution_time = 0" &

# Hang the test itself far past `clickhouse-test --timeout` so it is reported as
# a timeout too.
sleep 100000

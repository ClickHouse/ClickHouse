#!/usr/bin/env bash
# Tags: long, no-debug, no-fasttest
# no-fasttest: needs the AI SQL generator (`ENABLE_CLIENT_AI`), which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-test` dumps `$CLICKHOUSE_TMP/<testcase>.debuglog` into the failure report, and the
# testcase name is the name of this shell script - the Python part cannot derive it from its own
# file name, so pass the path down.
export CLICKHOUSE_TEST_DEBUG_LOG="$CLICKHOUSE_TMP/$(basename "${BASH_SOURCE[0]}").debuglog"

python3 "$CUR_DIR"/05025_client_hints_prepopulated_query.python

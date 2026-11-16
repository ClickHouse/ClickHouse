#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query-local `SETTINGS dialect = ...` must not change how the very query carrying it is parsed:
# the text was already accepted with the session dialect, and the setting only applies to the
# statements that follow. The client pins the outbound dialect to the one the text was accepted
# with, so the other side re-parses it the same way.

${CLICKHOUSE_LOCAL} -q "SELECT 'local_query_local_dialect', 1 SETTINGS dialect = 'kusto'"
${CLICKHOUSE_LOCAL} -q "SELECT 'local_query_local_dialect_prql', 2 SETTINGS dialect = 'prql'"
${CLICKHOUSE_CLIENT} -q "SELECT 'client_query_local_dialect', 3 SETTINGS dialect = 'kusto'"

# The setting still takes effect for the statements that follow it.
${CLICKHOUSE_LOCAL} --multiquery -q "SET dialect = 'kusto'; SET allow_experimental_kusto_dialect = 1; print 'kusto_after_set'"

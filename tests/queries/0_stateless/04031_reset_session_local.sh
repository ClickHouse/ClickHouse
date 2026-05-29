#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# clickhouse-local doesn't have a real session in the TCP sense, but it shares
# the InterpreterResetSessionQuery / Context::resetToUserDefaults code path with
# the server. Smoke that `RESET SESSION` doesn't crash and clears in-process
# state.

${CLICKHOUSE_LOCAL} -m --query "
SET max_threads = 999;
SET param_x = 'mango';
CREATE TEMPORARY TABLE t (x Int) ENGINE = Memory;
INSERT INTO t VALUES (1), (2);
SELECT 'before:', getSetting('max_threads'), {x:String}, count() FROM t;
RESET SESSION;
SELECT 'after:', getSetting('max_threads') = 999;
"

# Database restoration over the local/embedded path. `USE` sets the override
# on `LocalConnection`, which forces it onto every query context. After
# `RESET SESSION` the client must re-sync its connection-side database to the
# reset baseline rather than keep echoing the dirty `USE`-d database — the
# post-reset `currentDatabase()` probe must bypass the stale override.
${CLICKHOUSE_LOCAL} -m --query "
CREATE DATABASE local_reset_db;
USE local_reset_db;
SELECT 'db after use:', currentDatabase() = 'local_reset_db';
RESET SESSION;
SELECT 'db after reset, back off the dirty db:', currentDatabase() != 'local_reset_db';
"

# Startup settings applied only to the client context (here
# `implicit_table_at_top_level`, set from `--file`/`--structure`) must survive
# `RESET SESSION`. `clickhouse-local` runs queries from `LocalConnection`'s
# session context, which `resetToUserDefaults` rebuilds from the global context
# + profile — dropping client-only startup settings unless `LocalConnection`
# re-applies its startup baseline on reset. Without that, the post-reset value
# below would fall back to the empty default instead of 'table'.
DATA_FILE="${CLICKHOUSE_TMP}/04031_local_input.tsv"
printf '1\n2\n3\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --file="${DATA_FILE}" --input-format TSV --structure 'x Int' -m --query "
SELECT 'implicit table before:', getSetting('implicit_table_at_top_level');
RESET SESSION;
SELECT 'implicit table after:', getSetting('implicit_table_at_top_level');
"
rm -f "${DATA_FILE}"

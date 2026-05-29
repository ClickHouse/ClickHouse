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

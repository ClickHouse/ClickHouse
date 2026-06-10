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

# Command-line settings the user's profile does not override survive
# `RESET SESSION`: they are baked into the global context at startup, and
# `resetToUserDefaults` rebuilds the session settings from that global context.
# (A setting the profile DOES set is instead re-derived from the profile on
# reset — documented as a known limitation on the statement page.)
# `max_block_size` is not touched by the default profile, so the command-line
# value 12345 must come back after a session `SET` + reset.
${CLICKHOUSE_LOCAL} --max_block_size 12345 -m --query "
SELECT 'cmdline before:', getSetting('max_block_size');
SET max_block_size = 1;
RESET SESSION;
SELECT 'cmdline after reset:', getSetting('max_block_size');
"

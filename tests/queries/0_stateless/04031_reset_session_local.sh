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

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A MATERIALIZED column that reads an EPHEMERAL column cannot be recomputed by a mutation, so
# `CLEAR COLUMN` of one of its regular inputs silently leaves the on-disk value stale. The server
# warns about it, and the warning has to be decided over the same transitive closure the recompute
# uses: `m2` below reaches the cleared `x` only through `m1`, which the mutation does recompute, so
# a check against the directly cleared columns alone stays silent.

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_stale_warning;

CREATE TABLE t_stale_warning
(
    x Int32,
    e Int32 EPHEMERAL 0,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + e
)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY tuple()
-- The runner randomizes both of these together with \`min_bytes_for_wide_part\`, and on a Wide part
-- with either block column on the mutation does not recompute MATERIALIZED columns at all, so the
-- \`m1\` hop this test walks through would never run.
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_stale_warning (x, e) VALUES (1, 7);

SET mutations_sync = 2;
ALTER TABLE t_stale_warning CLEAR COLUMN x IN PARTITION tuple();

-- \`m1\` is recomputed from the cleared \`x\`; \`m2\` keeps the value stored at INSERT time.
SELECT x, m1, m2 FROM t_stale_warning;
"

# The interpreter runs inside the background mutate task, so its warning never reaches the client
# that issued the ALTER. The logger name carries the table, which isolates the rows well enough.
${CLICKHOUSE_CLIENT} -q "
SYSTEM FLUSH LOGS text_log;
SELECT 'warned about m2', count() >= 1
FROM system.text_log
WHERE logger_name = 'MutationsInterpreter(${CLICKHOUSE_DATABASE}.t_stale_warning)'
  AND level = 'Warning'
  AND message LIKE 'MATERIALIZED column \'m2\' depends on both EPHEMERAL and regular%';

DROP TABLE t_stale_warning;
"

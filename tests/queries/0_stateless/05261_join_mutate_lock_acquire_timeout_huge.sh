#!/usr/bin/env bash
# `ALTER TABLE ... DELETE` on a `Join` table waits up to `lock_acquire_timeout` for the inserts that
# started before it. The setting is user-controlled; a huge value must be clamped before it reaches
# `condition_variable::wait_for`, which converts it to nanoseconds and would otherwise overflow into
# an already-expired wait and fail the mutation with `DEADLOCK_AVOIDED` immediately.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS j;
CREATE TABLE j (id UInt64, v String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO j SELECT number, toString(number) FROM numbers(100);
"

# The rows of this insert arrive one at a time, long after its sink was created.
insert_query_id="insert_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "${insert_query_id}" -q "
INSERT INTO j SELECT number + 1000, toString(sleepEachRow(0.3)) FROM numbers(5) SETTINGS max_block_size = 1, max_threads = 1;
" &
insert_pid=$!

# A row that the insert has already read proves that its sink exists, so the mutation has to wait.
for _ in {1..600}
do
    [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT max(read_rows) FROM system.processes WHERE query_id = '${insert_query_id}'")" != "0" ]] && break
    kill -0 ${insert_pid} 2>/dev/null || break
    sleep 0.05
done

# 10^12 seconds is 10^15 milliseconds, i.e. 10^21 nanoseconds - beyond the range of `Int64`.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE j DELETE WHERE id < 50 SETTINGS lock_acquire_timeout = 1000000000000;"

wait ${insert_pid}

${CLICKHOUSE_CLIENT} -q "
SELECT 'in memory', count(), min(id), max(id), countIf(id >= 1000) FROM (SELECT id FROM j);
DETACH TABLE j;
ATTACH TABLE j;
SELECT 'after a reload', count(), min(id), max(id), countIf(id >= 1000) FROM (SELECT id FROM j);
DROP TABLE j;
"

#!/usr/bin/env bash
# An insert into a `Join` table is assigned its file when it starts, and inserts its rows and
# publishes the file later, on its own. An `ALTER TABLE ... DELETE` that ran in the meantime used to
# take its snapshot of the rows and replace the persisted files regardless: depending on the timing,
# the rows of the insert were missing from the table, or its file was published next to the
# replacement and the rows were loaded twice after a restart. The mutation now waits for the inserts
# that started before it, so the outcome is the same whichever of the two starts first.

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

# The sink is created while the pipeline is built, so a row that the insert has already read proves
# that the sink of this insert exists - which is the interleaving the mutation has to handle. Waiting
# for a fixed time instead would let the `ALTER` win the race on a loaded machine and the test pass
# without ever exercising it.
for _ in {1..600}
do
    [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT max(read_rows) FROM system.processes WHERE query_id = '${insert_query_id}'")" != "0" ]] && break
    kill -0 ${insert_pid} 2>/dev/null || break
    sleep 0.05
done

${CLICKHOUSE_CLIENT} -q "ALTER TABLE j DELETE WHERE id < 50;"

wait ${insert_pid}

${CLICKHOUSE_CLIENT} -q "
SELECT 'in memory', count(), min(id), max(id), countIf(id >= 1000) FROM (SELECT id FROM j);
DETACH TABLE j;
ATTACH TABLE j;
SELECT 'after a reload', count(), min(id), max(id), countIf(id >= 1000) FROM (SELECT id FROM j);
DROP TABLE j;
"

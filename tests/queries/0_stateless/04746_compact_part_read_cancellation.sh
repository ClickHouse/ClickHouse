#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# no-random-settings: a random max_execution_time / max_rows_to_read would end the queries
# instead of the limits set here, and max_block_size must stay large enough for one block to
# span every mark.
# no-random-merge-tree-settings: the read must go through a Compact part with
# index_granularity = 1, and both are randomized settings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_compact_read_cancel SYNC"

# ReplicatedMergeTree so that a wrongly reported broken part is observable: its
# broken_part_callback enqueues a part check, while plain MergeTree's is a no-op.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_compact_read_cancel (k UInt64, a String, b String, c String, d String, e String, f String, g String, h String, i String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_compact_read_cancel', 'r1')
ORDER BY k
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000
"

# One Compact part with 400k marks. index_granularity = 1 plus a block size covering the whole
# part means a single readRows call has to walk every mark, which takes many seconds.
${CLICKHOUSE_CLIENT} \
    --max_block_size 400000 --max_insert_block_size 400000 \
    --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 \
    -q "INSERT INTO t_compact_read_cancel
        SELECT number, s, s, s, s, s, s, s, s, s FROM (SELECT number, repeat('x', 30) AS s FROM numbers(400000))"

${CLICKHOUSE_CLIENT} -q "
SELECT 'part type', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_read_cancel' AND active"

read_query="SELECT count(), sum(length(a)+length(b)+length(c)+length(d)+length(e)+length(f)+length(g)+length(h)+length(i)) FROM t_compact_read_cancel"

# Only readRows() appends this diagnostic, so its presence proves the query stopped inside the
# block read rather than after it. The wall clock is not asserted: it depends on the machine,
# while the interrupt site does not.
inside_part_read() {
    grep -qF 'While reading part' "$1" && echo 'inside the part read' || echo "not interrupted inside the part read: $(head -c 400 "$1")"
}

timeout_err="${CLICKHOUSE_TMP}/04746_timeout_err.txt"
${CLICKHOUSE_CLIENT} --max_block_size 100000000 --preferred_block_size_bytes 0 --max_threads 1 \
    --max_execution_time 1 --timeout_overflow_mode throw \
    -q "$read_query" >/dev/null 2>"$timeout_err"
echo -n 'timeout observed '
inside_part_read "$timeout_err"

# The whole point of the check is that max_execution_time is respected, so also require the
# reported elapsed to stay near the limit. 5x the 1s limit is far above the fixed server (which
# stops one mark past the limit) and far below an uninterrupted read of this part.
elapsed=$(sed -nE 's/.*elapsed ([0-9]+)\.[0-9]+ ms.*/\1/p' "$timeout_err" | head -1)
if [ -n "$elapsed" ] && [ "$elapsed" -lt 5000 ]; then
    echo 'timeout honoured'
else
    echo "timeout overshot: elapsed=${elapsed}ms"
fi

query_id="compact_read_cancel_${CLICKHOUSE_DATABASE}_$$"
kill_err="${CLICKHOUSE_TMP}/04746_kill_err.txt"
${CLICKHOUSE_CLIENT} --query_id "$query_id" \
    --max_block_size 100000000 --preferred_block_size_bytes 0 --max_threads 1 \
    -q "$read_query" >/dev/null 2>"$kill_err" &
select_pid=$!

# Wait until the query is inside the single block read: it decompresses blocks
# (CompressedReadBufferBlocks grows) while no row has reached the pipeline yet (read_rows = 0),
# so the cancel has to be observed inside readRows and not between blocks.
reading=0
for _ in $(seq 1 600); do
    reading=$(${CLICKHOUSE_CLIENT} -q "
        SELECT ProfileEvents['CompressedReadBufferBlocks'] > 1000 AND read_rows = 0
        FROM system.processes WHERE query_id = '$query_id'")
    [ "$reading" = "1" ] && break
    sleep 0.1
done

if [ "$reading" != "1" ]; then
    echo 'did not observe the in-block read phase'
else
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
    echo -n 'kill observed '
    inside_part_read "$kill_err"
fi

wait "$select_pid" 2>/dev/null
rm -f "$timeout_err" "$kill_err"

# A cancelled read says nothing about the part's health: it must stay active, must not be
# detached, and must not have been queued for a check.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
SELECT 'active parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_read_cancel' AND active"
${CLICKHOUSE_CLIENT} -q "
SELECT 'detached parts', count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_compact_read_cancel'"
# Scope by currentDatabase(): text_log is server-wide, so an unscoped match would also count
# rows left by other runs of this test.
${CLICKHOUSE_CLIENT} -q "
SELECT 'part checks', count() FROM system.text_log
WHERE event_date >= yesterday()
  AND logger_name LIKE concat(currentDatabase(), '.t_compact_read_cancel%PartCheckThread%')
  AND message LIKE 'Enqueueing%for check%'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_compact_read_cancel SYNC"

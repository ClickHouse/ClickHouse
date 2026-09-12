#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# no-random-settings: the read-speed family (local_filesystem_read_method, max_read_buffer_size,
# enable_filesystem_cache, use_page_cache_for_local_disks,
# merge_tree_compact_parts_min_granules_to_multibuffer_read) is randomized, and a fast enough
# combination lets the unpatched read finish before a cancel is delivered, which stops the
# assertions discriminating. max_block_size is randomized too, but every query that depends on it
# sets it.
# The text arm additionally depends on use_skip_indexes_on_data_read and
# query_plan_direct_read_from_text_index, which are randomized too; those it pins per query,
# because each one on its own makes that assertion pass with the fix reverted.
# no-random-merge-tree-settings: the read must go through a Compact part with
# index_granularity = 1, and both that and min_bytes_for_wide_part are randomized. Redundant
# today, since no-random-settings already disables the merge-tree randomizer, but kept so the
# requirement is explicit.

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

# One Compact part with 100k marks. index_granularity = 1 plus a block size covering the whole
# part means a single readRows call has to walk every mark, which takes many seconds.
${CLICKHOUSE_CLIENT} \
    --max_block_size 100000 --max_insert_block_size 100000 \
    --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 \
    -q "INSERT INTO t_compact_read_cancel
        SELECT number, s, s, s, s, s, s, s, s, s FROM (SELECT number, repeat('x', 30) AS s FROM numbers(100000))"

${CLICKHOUSE_CLIENT} -q "
SELECT 'part type', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_read_cancel' AND active"

read_query="SELECT count(), sum(length(a)+length(b)+length(c)+length(d)+length(e)+length(f)+length(g)+length(h)+length(i)) FROM t_compact_read_cancel"

# This diagnostic is appended by MergeTreeReadersChain::read, whose try covers only the in-block
# startReadingChain call, so its presence proves the query stopped inside the block read rather
# than after it. The wall clock is not asserted: it depends on the machine, while the interrupt
# site does not.
inside_part_read() {
    grep -qF 'While reading part' "$1" && echo 'inside the part read' || echo "not interrupted inside the part read: $(head -c 400 "$1")"
}

# max_execution_time runs from the start of the query, so loading the marks and analysing the index
# are charged against it, and several checks fire before the read begins. On a loaded runner the
# work before the read can outlast a fixed limit, and the deadline then fires while the query is
# still being prepared, so the read is never entered and the diagnostic above cannot appear. Retry
# with a longer limit until the deadline lands inside the read, keyed on that diagnostic itself:
# it is the only signal that reports where the query stopped. A limit that stops timing out at all
# is already too generous, so the ladder ends there. The elapsed value is not asserted, because it
# is everything before the read plus one mark, so it tracks machine load.
timeout_err="${CLICKHOUSE_TMP}/04746_timeout_err.txt"
for limit in 1 2 4 8 16 32 64; do
    ${CLICKHOUSE_CLIENT} --max_block_size 100000000 --preferred_block_size_bytes 0 --max_threads 1 \
        --max_execution_time "$limit" --timeout_overflow_mode throw \
        -q "$read_query" >/dev/null 2>"$timeout_err"
    grep -qF 'While reading part' "$timeout_err" && break
    grep -q 'Timeout exceeded' "$timeout_err" || break
done

echo -n 'timeout observed '
inside_part_read "$timeout_err"

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

# The text index reader has the same unchecked per-mark loop, and it is a separate reader: the
# compact reader's check cannot fire for a query that reads no physical column, because such a
# main reader is left out of the readers chain entirely.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_text_read_cancel SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_text_read_cancel (k UInt64, s String, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000
"

${CLICKHOUSE_CLIENT} \
    --max_block_size 100000 --max_insert_block_size 100000 \
    --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 \
    -q "INSERT INTO t_text_read_cancel
        SELECT number, concat('tok', toString(number % 1000), ' filler text here') FROM numbers(100000)"

${CLICKHOUSE_CLIENT} -q "
SELECT 'text part type', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_read_cancel' AND active"

# Every row carries 'filler', so no mark is pruned and the walk covers all 100k of them. Each
# extra predicate adds a virtual column whose posting list is materialized per mark, and the walk
# costs about 37 ms per predicate, so the count sets how long the read lasts. It has to stay long
# relative to the handshake below, which needs a few client round-trips: at 30 predicates the read
# takes 1.1 s and the cancel can arrive after it has already finished.
text_pred="1"
for i in $(seq 1 240); do
    text_pred="$text_pred AND hasAnyTokens(s, ['filler', 'tok$i'])"
done
text_query="SELECT count() FROM t_text_read_cancel WHERE $text_pred"

# Assert the text index is what serves the query: if it silently fell back to a full scan the
# reader under test would never run. Analysing the predicate costs about as much per term as
# reading does, so this asks the same question of a single term: whether the index is usable for
# this predicate shape does not depend on how many terms are conjoined.
${CLICKHOUSE_CLIENT} --use_skip_indexes_on_data_read 0 --query_plan_direct_read_from_text_index 1 -q "
SELECT 'text index used', count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text_read_cancel WHERE hasAnyTokens(s, ['filler', 'tok1']))
WHERE explain ILIKE '%Name: idx_s%'"

# Both settings are pinned per query because both are randomized and each one silently disarms the
# assertion below.
# use_skip_indexes_on_data_read = 0: with it enabled a MergeTreeReaderIndex is placed ahead of the
# text reader in the chain, so the text reader is driven by continueReadingChain, which sits
# outside the try that appends the token. With it disabled the text reader is the first reader and
# the token reports its interrupt site.
# query_plan_direct_read_from_text_index = 1: with it disabled the text reader is not used at all,
# the predicate is evaluated over the physical column, and the token then comes from the compact
# reader's own check, so the token would report an interrupt with the text-index check reverted.
# Cancelled by KILL rather than by a deadline, for the reason given above the compact timeout arm:
# analysing this index costs more than a fixed limit on a loaded runner, so a deadline can expire
# before the read starts.
#
# The handshake below cannot prove the read has begun: SelectedMarks is incremented once index
# analysis is over but before the pipeline runs, and no counter advances while the reader walks
# marks, so every available signal either precedes the read or appears only after it ends. A cancel
# sent on the earlier one lands before the reader is entered whenever the gap between them
# stretches, which is what a loaded runner does. So retry, keyed on the same interrupt-site
# diagnostic the compact arms use, growing the pause before the cancel until it lands inside.
# Every pause stays below how long the read lasts, because one that outlasted it would let the query
# finish and leave the diagnostic absent for the opposite reason, which this retry cannot tell apart
# and would answer by pausing longer still. Load only slows the walk, so that margin is smallest on
# an idle machine.
text_err="${CLICKHOUSE_TMP}/04746_text_err.txt"
text_reading=0
for text_settle in 0 0.25 0.5 1 2 4; do
    text_query_id="text_read_cancel_${CLICKHOUSE_DATABASE}_$$_${text_settle}"
    ${CLICKHOUSE_CLIENT} --query_id "$text_query_id" \
        --max_block_size 100000000 --preferred_block_size_bytes 0 --max_threads 1 \
        --use_skip_indexes_on_data_read 0 --query_plan_direct_read_from_text_index 1 \
        -q "$text_query" >/dev/null 2>"$text_err" &
    text_pid=$!

    # Every mark has been selected, so analysis is over, while no row has reached the pipeline yet
    # (read_rows = 0), so the query has not delivered a block.
    text_reading=0
    for _ in $(seq 1 600); do
        text_reading=$(${CLICKHOUSE_CLIENT} -q "
            SELECT ProfileEvents['SelectedMarks'] > 0 AND read_rows = 0
            FROM system.processes WHERE query_id = '$text_query_id'")
        [ "$text_reading" = "1" ] && break
        sleep 0.1
    done

    if [ "$text_reading" != "1" ]; then
        wait "$text_pid" 2>/dev/null
        break
    fi

    [ "$text_settle" != "0" ] && sleep "$text_settle"
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$text_query_id' SYNC FORMAT Null"
    wait "$text_pid" 2>/dev/null
    grep -qF 'While reading part' "$text_err" && break
done

echo -n 'text kill observed '
if [ "$text_reading" != "1" ]; then
    echo 'did not observe the in-block read phase'
else
    inside_part_read "$text_err"
fi
rm -f "$text_err"

# The line above proves the query stopped inside a block read, and 'text index used' proves the
# index pruned granules, but neither proves the text index READER served the query: the direct-read
# rewrite is decided separately from pruning. This event is incremented only at the top of
# MergeTreeReaderTextIndex::readRows, and its scope guard reports during unwinding, so it is
# recorded even though this query leaves that function by throwing. read_rows stays 0 because the
# interrupted read delivers no block, which an uninterrupted walk would.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT 'text index reader ran', ProfileEvents['TextIndexReaderTotalMicroseconds'] > 0, read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '$text_query_id'
  AND type = 'ExceptionWhileProcessing' AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_text_read_cancel SYNC"

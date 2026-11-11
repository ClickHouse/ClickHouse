#!/usr/bin/env bash

# An `INSERT` keeps the metadata snapshot it took at query start, so the parts it writes after a
# concurrent `ALTER TABLE ... DROP PROJECTION` commits still materialize the dropped projection, both
# as a `.proj` directory and as an entry in the part's `checksums.txt`. Nothing rewrites those parts
# afterwards - the drop's own cleanup mutation was created before their block numbers - and every later
# mutation hardlinks the directory into the mutated part. `checkDataPart` tolerates a projection
# directory that the current metadata does not know, but it used to leave it in the unexpected-files
# set, so a mutated part (checked with checksums required) was reported broken forever although every
# data file is intact.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

orphaned=0

for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS t_05201;
        -- Keep the affected parts around: a merge would rebuild them without the dropped projection.
        CREATE TABLE t_05201 (id UInt64, val UInt64, part UInt8) ENGINE = MergeTree PARTITION BY part ORDER BY id
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
        ALTER TABLE t_05201 ADD PROJECTION p1 (SELECT part, sum(val) GROUP BY part);
    "

    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO t_05201 SELECT number, sleepEachRow(0.05) * 0 + number, 0 FROM numbers(100)
        SETTINGS max_block_size = 10, min_insert_block_size_rows = 10, max_insert_threads = 1
    " &
    insert_pid=$!

    sleep 1.5
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_05201 DROP PROJECTION p1"
    wait $insert_pid

    orphaned=$(${CLICKHOUSE_CLIENT} -q "
        SELECT countIf(has(projections, 'p1')) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_05201' AND active
    ")

    [ "$orphaned" -gt 0 ] && break
done

if [ "$orphaned" -gt 0 ]; then echo "a part kept the dropped projection"; else echo "the race did not happen"; fi

echo "rows: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05201")"
echo "check before a mutation: $(${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_05201 SETTINGS check_query_single_value_result = 1")"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_05201 UPDATE val = val + 1 WHERE id < 50 SETTINGS mutations_sync = 2"

echo "rows after the mutation: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_05201")"
echo "check after the mutation: $(${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_05201 SETTINGS check_query_single_value_result = 1")"
echo "parts reported broken: $(${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_05201 SETTINGS check_query_single_value_result = 0" | awk -F'\t' '$2 == 0' | wc -l | tr -d ' ')"

# The projection is gone from the table, so a query cannot use it, and reading is unaffected.
echo "sum: $(${CLICKHOUSE_CLIENT} -q "SELECT sum(val) FROM t_05201")"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_05201"

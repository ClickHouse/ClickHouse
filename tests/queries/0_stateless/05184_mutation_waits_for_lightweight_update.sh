#!/usr/bin/env bash
# A heavyweight `ALTER ... UPDATE` that ran while a lightweight `UPDATE` was still writing its patch
# part silently dropped the acknowledged lightweight update on a plain `MergeTree`: the mutation read
# the part without the patch and wrote a part at a higher data version, which the patch no longer
# applied to. `ReplicatedMergeTree` postpones such a mutation while a lightweight update with a lower
# block number is uncommitted; the plain engine selected the part regardless.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_mutation_waits_lwu;
CREATE TABLE t_mutation_waits_lwu (id UInt64, v UInt64, w UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_mutation_waits_lwu SELECT number, 1, 0 FROM numbers(20000);
"

# `sleepEachRow` only widens the window in which the update holds its block number without having
# committed its patch part.
${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "
UPDATE t_mutation_waits_lwu SET v = 2 WHERE v = 1 AND sleepEachRow(0.00005) = 0
" &

for _ in {1..100}
do
    running=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query LIKE 'UPDATE t_mutation_waits_lwu%'")
    if [[ "$running" -gt 0 ]]; then
        break
    fi
    sleep 0.05
done

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_mutation_waits_lwu UPDATE w = 5 WHERE 1 SETTINGS mutations_sync = 1"
wait

${CLICKHOUSE_CLIENT} -q "
SELECT 'the lightweight update survived', count() FROM t_mutation_waits_lwu WHERE v = 2;
SELECT 'and the mutation applied', count() FROM t_mutation_waits_lwu WHERE w = 5;
SELECT 'rows', count() FROM t_mutation_waits_lwu;
DROP TABLE t_mutation_waits_lwu;
"

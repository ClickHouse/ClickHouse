#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-object-storage

# Regression for a Wide part that has a Dynamic column but no columns_substreams.txt, as written by
# servers from before that file existed (Dynamic became production-ready in 25.3, the file was added
# to Wide parts in 25.8). For such a part the mutation stream-accounting cannot enumerate the
# data-dependent substreams of the Dynamic column (variant_discr, ...) without a deserialization
# state, so a partial mutation could leave one of those streams neither rewritten nor hardlinked.
# The mutation must instead rewrite the whole part. We simulate the old part by deleting
# columns_substreams.txt and reloading the table, then run a partial mutation that does NOT touch the
# Dynamic column and validate the resulting part with CHECK TABLE.
# See https://github.com/ClickHouse/ClickHouse/issues/107561

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_dyn_old_part"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_dyn_old_part (id UInt64, s UInt64, y Dynamic(max_types=3))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_dyn_old_part SELECT number, number, number::Int64 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_dyn_old_part SELECT number, number, 's' || number FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_dyn_old_part FINAL"

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_dyn_old_part' AND active")

echo -n "columns_substreams.txt present before: "
test -f "${DATA_PATH}columns_substreams.txt" && echo 1 || echo 0

# Detach the table, delete columns_substreams.txt to simulate a part written before the file existed,
# then reload it from disk.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_dyn_old_part"
rm -f "${DATA_PATH}columns_substreams.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_dyn_old_part"

echo -n "columns_substreams.txt present after delete+attach: "
test -f "${DATA_PATH}columns_substreams.txt" && echo 1 || echo 0

# Partial mutation that does NOT touch the Dynamic column. Because the source part has a Dynamic
# column with no recorded substreams, the whole part must be rewritten.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_dyn_old_part UPDATE s = s + 1 WHERE id % 2 = 0 SETTINGS mutations_sync = 2"

echo "Data after mutation:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(y IS NOT NULL), countIf(s = id + (id % 2 = 0)) FROM t_dyn_old_part"
${CLICKHOUSE_CLIENT} --query "SELECT dynamicType(y) AS t, count() FROM t_dyn_old_part GROUP BY t ORDER BY t"

echo -n "CHECK TABLE result: "
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_dyn_old_part SETTINGS check_query_single_value_result = 1"

# The rewritten part is in the modern format again: columns_substreams.txt exists and records the
# Dynamic column's variant_discr substream.
NEW_DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_dyn_old_part' AND active")
echo -n "columns_substreams.txt regenerated with variant_discr: "
grep -q "variant_discr" "${NEW_DATA_PATH}columns_substreams.txt" 2>/dev/null && echo 1 || echo 0

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_dyn_old_part"

#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-object-storage

# Companion to 04401: a Wide part with a plain Map column but no columns_substreams.txt (as written by
# servers from before that file existed) must NOT be force-rewritten by a partial mutation of an
# unrelated column. A basic Map serialization enumerates all of its physical streams without a
# deserialization state (unlike Dynamic/JSON, whose data-dependent substreams require the state), so a
# partial mutation can account for every stream and stay on the cheap partial path. We simulate the old
# part by deleting columns_substreams.txt and reloading the table, run a partial mutation that does NOT
# touch the Map column, validate with CHECK TABLE, and assert the rewritten part did NOT regenerate
# columns_substreams.txt (which would have meant a needless full rewrite of all the Map data).
# See https://github.com/ClickHouse/ClickHouse/issues/107561

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_map_old_part"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_map_old_part (id UInt64, s UInt64, m Map(String, String))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_map_old_part SELECT number, number, map('id', toString(number), 'k', toString(number * 2)) FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_map_old_part FINAL"

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_map_old_part' AND active")

echo -n "columns_substreams.txt present before: "
test -f "${DATA_PATH}columns_substreams.txt" && echo 1 || echo 0

# Detach the table, delete columns_substreams.txt to simulate a part written before the file existed,
# then reload it from disk.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_map_old_part"
rm -f "${DATA_PATH}columns_substreams.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_map_old_part"

echo -n "columns_substreams.txt present after delete+attach: "
test -f "${DATA_PATH}columns_substreams.txt" && echo 1 || echo 0

# Partial mutation that does NOT touch the Map column. The Map's streams are fully enumerable without a
# deserialization state, so there is no correctness need to rewrite the whole part: it must stay on the
# partial path.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_map_old_part UPDATE s = s + 1 WHERE id % 2 = 0 SETTINGS mutations_sync = 2"

echo "Data after mutation:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(s = id + (id % 2 = 0)), countIf(m['id'] = toString(id) AND m['k'] = toString(id * 2)) FROM t_map_old_part"

echo -n "CHECK TABLE result: "
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_map_old_part SETTINGS check_query_single_value_result = 1"

# A partial mutation of a part with no columns_substreams.txt does not write one (it is only filled from
# a non-empty source). So the absence of the file proves the part stayed on the partial path; if the Map
# column had wrongly forced a full rewrite, the part would have regained columns_substreams.txt.
NEW_DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_map_old_part' AND active")
echo -n "Stayed on partial path (no columns_substreams.txt regenerated): "
test -f "${NEW_DATA_PATH}columns_substreams.txt" && echo 0 || echo 1

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_map_old_part"

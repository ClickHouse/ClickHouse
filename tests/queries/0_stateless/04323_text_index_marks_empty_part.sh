#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_text_idx_empty SYNC"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_text_idx_empty
(
    s FixedString(37),
    INDEX idx s TYPE text(tokenizer = array()) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS prewarm_mark_cache = true, compress_marks = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number), 37) FROM numbers(5)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 5), 37) FROM numbers(5)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_text_idx_empty FINAL"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_text_idx_empty DELETE WHERE 1 SETTINGS mutations_sync = 2"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_text_idx_empty'")
PART_NAME=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_text_idx_empty' AND active")
PART_DIR="${DATA_PATH}${PART_NAME}"

${CLICKHOUSE_CLIENT} -q "SELECT rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_text_idx_empty' AND active"

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_text_idx_empty"

printf '\x00' >> "${PART_DIR}/skp_idx_idx.mrk4"
rm "${PART_DIR}/checksums.txt"

${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "ATTACH TABLE t_text_idx_empty"

${CLICKHOUSE_CLIENT} -q "SYSTEM PREWARM MARK CACHE t_text_idx_empty"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_text_idx_empty"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_text_idx_empty WHERE has(['anything'], s)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_text_idx_empty SYNC"

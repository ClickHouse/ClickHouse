#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m -q """
DROP TABLE IF EXISTS with_lonely;

CREATE TABLE with_lonely
(
    id UInt64,
    dt Date,
    val UInt64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY dt
ORDER BY (id);
"""

create_optimize_partition() {
    ${CLICKHOUSE_CLIENT} -m -q """
    INSERT INTO with_lonely SELECT number, '$1', number*10, 0 FROM numbers(10);
    INSERT INTO with_lonely SELECT number+500000, '$1', number*10, 1 FROM numbers(10);
    """
    cnt=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'with_lonely' AND partition = '$1' AND active")
    while [ "$cnt" -ne "1" ]; do
        ${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE with_lonely PARTITION '$1' FINAL;"
        cnt=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'with_lonely' AND partition = '$1' AND active")
    done
}

create_optimize_partition "2022-10-28"
create_optimize_partition "2022-10-29"
create_optimize_partition "2022-10-30"
create_optimize_partition "2022-10-31"

${CLICKHOUSE_CLIENT} -m -q """
SYSTEM STOP MERGES with_lonely;

INSERT INTO with_lonely SELECT number, '2022-11-01', number*10, 0 FROM numbers(10);
INSERT INTO with_lonely SELECT number+50000, '2022-11-01', number*10, 1 FROM numbers(10);
INSERT INTO with_lonely SELECT number+60000, '2022-11-01', number*10, 2 FROM numbers(10);
"""

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --do_not_merge_across_partitions_select_final 1"

# mix lonely parts and non-lonely parts
${CLICKHOUSE_CLIENT} --max_bytes_to_read 2000 -q "SELECT max(val), count(*) FROM with_lonely FINAL;"
# only lonely parts
${CLICKHOUSE_CLIENT} --max_bytes_to_read 1000 -q "SELECT max(val), count(*) FROM with_lonely FINAL WHERE dt < '2022-11-01';"
# only lonely parts but max_thread = 1, so reading lonely parts with in-order
${CLICKHOUSE_CLIENT} --max_threads 1 --max_bytes_to_read 1000 -q "SELECT max(val), count(*) FROM with_lonely FINAL WHERE dt < '2022-11-01';"

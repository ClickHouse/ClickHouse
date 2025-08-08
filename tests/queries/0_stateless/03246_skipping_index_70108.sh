#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -m "
    CREATE TABLE t
    (
        c0 UInt64,
        c1 String,
        INDEX c1_idx c1 TYPE set(666) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY c0;
    INSERT INTO t SELECT * FROM file('$CURDIR/data_i70108/repro.tsv.zstd');

SELECT count() FROM t WHERE c1 = 'dedenk1d4q' SETTINGS use_skip_indexes=1;
SELECT count() FROM t WHERE c1 = 'dedenk1d4q' SETTINGS use_skip_indexes=0;
"

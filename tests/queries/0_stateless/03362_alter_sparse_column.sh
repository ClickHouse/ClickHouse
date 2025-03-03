#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


set -eu

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t0;
"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t0 (
        k  Int,
        k1 Int,
        v  SimpleAggregateFunction(max, Int) CODEC(DoubleDelta, ZSTD(3)),
        v1 SimpleAggregateFunction(max, Int) CODEC(DoubleDelta, ZSTD(3)),

        PROJECTION prj (
            SELECT
                k1,
                min(v) AS min_v,
                max(v) AS max_v
            GROUP BY k1
        )
    )
    ENGINE=ReplicatedAggregatingMergeTree('/test/03362/{database}/t0', '1')
    PRIMARY KEY (k, k1)
    ORDER BY (k, k1)
    SETTINGS ratio_of_defaults_for_sparse_serialization=0, min_bytes_for_wide_part=0, min_bytes_for_full_part_storage=0, deduplicate_merge_projection_mode='rebuild';
"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t0 (k, k1, v, v1)
    VALUES (1, 1, ,), (2, 1, 1,), (3, 1, ,);
"

$CLICKHOUSE_CLIENT -q "
    SELECT * FROM system.parts
    WHERE database=currentDatabase() and table='t0'
    ORDER BY name;
"

path=$(
    $CLICKHOUSE_CLIENT -q "
    SELECT path FROM system.parts
    WHERE database=currentDatabase() and table='t0' and active = 1
    ORDER BY name;
"
)

echo "path=$path"
ls $path

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t0
    DROP PROJECTION prj
    SETTINGS alter_sync=0, mutations_sync=0;
"

path=$(
    $CLICKHOUSE_CLIENT -q "
    SELECT path FROM system.parts
    WHERE database=currentDatabase() and table='t0' and active = 1
    ORDER BY name;
"
)

echo "path=$path"
ls $path

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t0
    MODIFY COLUMN v SimpleAggregateFunction(max, Nullable(Int))
    SETTINGS alter_sync=2, mutations_sync=2;
"

$CLICKHOUSE_CLIENT -q "
    SELECT *FROM system.parts
    WHERE database=currentDatabase() and table='t0'
    ORDER BY name;
"

path=$(
    $CLICKHOUSE_CLIENT -q "
        SELECT path FROM system.parts
        WHERE database=currentDatabase() and table='t0' and active = 1
        ORDER BY name;
    "
)

echo "path=$path"
ls $path

echo "cat checksums.txt"
cat $path/checksums.txt | tail -c +29 | $CLICKHOUSE_COMPRESSOR --decompress | strings

$CLICKHOUSE_CLIENT -q "
    TRUNCATE TABLE t0;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t0;
"

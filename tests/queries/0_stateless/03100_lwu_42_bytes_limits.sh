#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_lwu_bytes_limits_2 SYNC;

    CREATE TABLE t_lwu_bytes_limits_2 (id UInt64, s String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_bytes_limits_2', '1') ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        max_uncompressed_bytes_in_patches = '100Ki',
        cleanup_delay_period = 1,
        max_cleanup_delay_period = 1,
        cleanup_delay_period_random_add = 0;

    SET enable_lightweight_update = 1;
    INSERT INTO t_lwu_bytes_limits_2 SELECT number, randomPrintableASCII(10) FROM numbers(2000);

    UPDATE t_lwu_bytes_limits_2 SET s = 'aaabbb' WHERE 1;
    UPDATE t_lwu_bytes_limits_2 SET s = 'cccddd' WHERE 1; -- { serverError TOO_LARGE_LIGHTWEIGHT_UPDATES }

    SELECT sum(data_uncompressed_bytes) < 100 * 1024
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_lwu_bytes_limits_2' AND active AND name LIKE 'patch%';

    SELECT s, count() FROM t_lwu_bytes_limits_2 GROUP BY s ORDER BY s;

    DETACH TABLE t_lwu_bytes_limits_2;
    ATTACH TABLE t_lwu_bytes_limits_2;

    UPDATE t_lwu_bytes_limits_2 SET s = 'cccddd' WHERE 1; -- { serverError TOO_LARGE_LIGHTWEIGHT_UPDATES }

    SELECT sum(data_uncompressed_bytes) < 100 * 1024
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_lwu_bytes_limits_2' AND active AND name LIKE 'patch%';

    SELECT s, count() FROM t_lwu_bytes_limits_2 GROUP BY s ORDER BY s;
    ALTER TABLE t_lwu_bytes_limits_2 APPLY PATCHES SETTINGS mutations_sync = 2;
"

for _ in {0..50}; do
    res=`$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_bytes_limits_2' AND active AND startsWith(name, 'patch')"`
    if [[ $res == "0" ]]; then
        break
    fi
    sleep 1.0
done

$CLICKHOUSE_CLIENT -q "
    SET enable_lightweight_update = 1;
    UPDATE t_lwu_bytes_limits_2 SET s = 'cccddd' WHERE 1;

    SELECT sum(data_uncompressed_bytes) < 100 * 1024
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_lwu_bytes_limits_2' AND active AND name LIKE 'patch%';

    SELECT s, count() FROM t_lwu_bytes_limits_2 GROUP BY s ORDER BY s;

    DROP TABLE t_lwu_bytes_limits_2 SYNC;
"


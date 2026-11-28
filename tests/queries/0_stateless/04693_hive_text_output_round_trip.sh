#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the HiveText input format requires USE_HIVE

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The documented HiveText compatibility contract: top-level scalar fields written with the default
# fields delimiter ('\x01') and the default rows delimiter ('\n') round-trip through
# SELECT ... FORMAT HiveText | INSERT ... FORMAT HiveText.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_hive_text_round_trip;
    CREATE TABLE t_hive_text_round_trip
    (
        u UInt64,
        i Int32,
        f Float64,
        s String,
        d Date,
        dt DateTime('UTC'),
        dec Decimal(18, 4),
        b Bool,
        n Nullable(String)
    )
    ENGINE = MergeTree ORDER BY u;

    INSERT INTO t_hive_text_round_trip VALUES
        (0, -1, 1.5, '', '2020-01-01', '2020-01-01 12:00:00', -1.2345, false, NULL),
        (1, 42, -0.25, 'hello world', '2031-12-31', '2031-12-31 23:59:59', 98765.4321, true, 'not null');
"

$CLICKHOUSE_CLIENT --query "SELECT * FROM t_hive_text_round_trip ORDER BY u FORMAT HiveText" \
    | $CLICKHOUSE_CLIENT --query "INSERT INTO t_hive_text_round_trip FORMAT HiveText"

# Every row must now be present exactly twice with identical values.
$CLICKHOUSE_CLIENT --query "
    SELECT count(), uniqExact(tuple(*)) FROM t_hive_text_round_trip;
    SELECT DISTINCT * FROM t_hive_text_round_trip ORDER BY u FORMAT TSV;
    DROP TABLE t_hive_text_round_trip;
"

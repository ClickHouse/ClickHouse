#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the HiveText input format requires USE_HIVE

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The HiveText output format always writes NULL as Hive's default null sequence '\N',
# and the HiveText input format always reads '\N' as NULL. Both sides must be
# independent of format_csv_null_representation, so the documented top-level scalar
# round-trip holds even when that setting is changed to a non-default token.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_hive_text_null_repr;
    CREATE TABLE t_hive_text_null_repr
    (
        u UInt64,
        n Nullable(String),
        m Nullable(Int32)
    )
    ENGINE = MergeTree ORDER BY u;

    INSERT INTO t_hive_text_null_repr VALUES
        (0, NULL, NULL),
        (1, 'not null', 42);
"

$CLICKHOUSE_CLIENT --format_csv_null_representation 'CUSTOM_NULL' --query "SELECT * FROM t_hive_text_null_repr ORDER BY u FORMAT HiveText" \
    | $CLICKHOUSE_CLIENT --format_csv_null_representation 'CUSTOM_NULL' --query "INSERT INTO t_hive_text_null_repr FORMAT HiveText"

# Every row must now be present exactly twice with identical values: the NULLs must
# round-trip as real NULLs (not the literal string '\N' or 'CUSTOM_NULL'), and the
# Nullable(Int32) must parse back as a number.
$CLICKHOUSE_CLIENT --query "
    SELECT count(), uniqExact(tuple(*)) FROM t_hive_text_null_repr;
    SELECT DISTINCT * FROM t_hive_text_null_repr ORDER BY u FORMAT TSV;
    DROP TABLE t_hive_text_null_repr;
"

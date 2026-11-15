#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ttl_only_drop_parts` trades the merges that delete expired rows for dropping whole parts, but a
# column TTL can only be honoured by rewriting the part. The setting must therefore not suppress the
# merges that clear an expired column, and it must not clear a column whose TTL has not expired yet.

# A table with a row TTL that has already expired and a column TTL that is not due yet. The column
# TTL must not start a rewrite, but it must not postpone dropping a part whose rows have all expired.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ttl_col_mixed;
    CREATE TABLE ttl_col_mixed
    (
        d Date,
        keep String,
        not_expired String TTL d + INTERVAL 100 YEAR
    )
    ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 DAY
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0;

    INSERT INTO ttl_col_mixed VALUES ('2020-01-01', 'keep', 'not_expired');"

# Once an unconditional row TTL has expired for the whole part, later conditional deletion and
# aggregation TTLs cannot require any rows to be retained either.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ttl_where_mixed;
    CREATE TABLE ttl_where_mixed
    (
        d Date,
        k UInt64
    )
    ENGINE = MergeTree ORDER BY k
    TTL d + INTERVAL 1 DAY,
        d + INTERVAL 100 YEAR DELETE WHERE k = 1
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0;

    INSERT INTO ttl_where_mixed VALUES ('2020-01-01', 1);

    DROP TABLE IF EXISTS ttl_group_by_mixed;
    CREATE TABLE ttl_group_by_mixed
    (
        d Date,
        k UInt64,
        value UInt64
    )
    ENGINE = MergeTree ORDER BY k
    TTL d + INTERVAL 1 DAY,
        d + INTERVAL 100 YEAR GROUP BY k SET d = max(d), value = sum(value)
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0;

    INSERT INTO ttl_group_by_mixed VALUES ('2020-01-01', 1, 1);"

for only_drop in 0 1
do
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS ttl_col_${only_drop};
        CREATE TABLE ttl_col_${only_drop}
        (
            d Date,
            keep String,
            expired String TTL d + INTERVAL 1 DAY,
            not_expired String TTL d + INTERVAL 100 YEAR
        )
        ENGINE = MergeTree ORDER BY d
        SETTINGS ttl_only_drop_parts = ${only_drop}, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0;

        INSERT INTO ttl_col_${only_drop} VALUES ('2020-01-01', 'keep', 'expired', 'not_expired');"
done

for table in ttl_col_mixed ttl_where_mixed ttl_group_by_mixed
do
    for _ in {1..300}
    do
        result=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM ${table}")
        [[ "$result" == "0" ]] && break
        sleep 0.3
    done
done

# The background TTL merge is asynchronous; wait for it instead of forcing it with OPTIMIZE, because
# OPTIMIZE bypasses the merge selector that this test is about.
for only_drop in 0 1
do
    for _ in {1..300}
    do
        result=$($CLICKHOUSE_CLIENT --query "SELECT expired = '' FROM ttl_col_${only_drop}")
        [[ "$result" == "1" ]] && break
        sleep 0.3
    done
done

for only_drop in 0 1
do
    echo "ttl_only_drop_parts = ${only_drop}"
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), keep, expired, not_expired FROM ttl_col_${only_drop} GROUP BY keep, expired, not_expired;"
done

echo "mixed row and column TTL"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ttl_col_mixed;"

echo "mixed unconditional and conditional row TTL"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ttl_where_mixed;"

echo "mixed unconditional and aggregation TTL"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ttl_group_by_mixed;"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE ttl_col_0;
    DROP TABLE ttl_col_1;
    DROP TABLE ttl_col_mixed;
    DROP TABLE ttl_where_mixed;
    DROP TABLE ttl_group_by_mixed;"

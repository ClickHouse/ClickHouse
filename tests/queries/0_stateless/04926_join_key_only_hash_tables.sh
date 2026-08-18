#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `LEFT ANTI` and `LEFT SEMI` without selected right columns run on hash tables that store the keys
# alone. Every query below must return the same thing whether or not those tables are used.
check()
{
    local with_key_only
    local with_mapped
    with_key_only=$($CLICKHOUSE_CLIENT --enable_join_key_only_hash_tables=1 --query "$1")
    with_mapped=$($CLICKHOUSE_CLIENT --enable_join_key_only_hash_tables=0 --query "$1")

    if [ "$with_key_only" == "$with_mapped" ]
    then
        echo "$with_key_only"
    else
        echo "MISMATCH for: $1"
        echo "key-only tables: $with_key_only"
        echo "mapped tables:   $with_mapped"
    fi
}

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE l_num (k UInt64, small UInt8, s String, lc LowCardinality(String), n Nullable(UInt64), v String) ENGINE = Memory;
    CREATE TABLE r_num (k UInt64, small UInt8, s String, lc LowCardinality(String), n Nullable(UInt64), p String) ENGINE = Memory;
    CREATE TABLE r_empty (k UInt64, p String) ENGINE = Memory;

    INSERT INTO l_num
        SELECT number, number % 7, toString(number), toString(number % 5), if(number % 4 = 0, NULL, number), concat('l', toString(number))
        FROM numbers(20);

    INSERT INTO r_num
        SELECT number * 3, (number * 3) % 7, toString(number * 3), toString(number % 3), if(number % 3 = 0, NULL, number * 3), 'r'
        FROM numbers(10);

    -- duplicate right keys must not change the result of either strictness. The payload is the same in
    -- the copy, so that which of the two rows a SEMI join picks does not show up in the result.
    INSERT INTO r_num SELECT k, small, s, lc, n, p FROM r_num;
"

echo "-- ANTI, key only"
check "SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k"
echo "-- ANTI, right column selected: always default"
check "SELECT k, p FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k"
echo "-- SEMI, key only"
check "SELECT k FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k"
echo "-- SEMI, right column selected"
check "SELECT k, p FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k"
echo "-- ANTI, right column selected, join_use_nulls"
check "SELECT k, p FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_use_nulls = 1"
echo "-- ANTI/SEMI with ON"
check "SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k"
check "SELECT count() FROM l_num AS l SEMI LEFT JOIN r_num AS r ON l.k = r.k"
echo "-- one-byte key"
check "SELECT small FROM l_num ANTI LEFT JOIN r_num USING (small) ORDER BY small"
check "SELECT small FROM l_num SEMI LEFT JOIN r_num USING (small) ORDER BY small"
echo "-- string key"
check "SELECT s FROM l_num ANTI LEFT JOIN r_num USING (s) ORDER BY s"
echo "-- LowCardinality key"
check "SELECT lc FROM l_num ANTI LEFT JOIN r_num USING (lc) ORDER BY lc"
check "SELECT lc FROM l_num SEMI LEFT JOIN r_num USING (lc) ORDER BY lc"
echo "-- nullable key"
check "SELECT n FROM l_num ANTI LEFT JOIN r_num USING (n) ORDER BY n"
check "SELECT n FROM l_num SEMI LEFT JOIN r_num USING (n) ORDER BY n"
echo "-- composite key"
check "SELECT k, s FROM l_num ANTI LEFT JOIN r_num USING (k, s) ORDER BY k"
echo "-- empty right table"
check "SELECT k FROM l_num ANTI LEFT JOIN r_empty USING (k) ORDER BY k"
check "SELECT k FROM l_num SEMI LEFT JOIN r_empty USING (k) ORDER BY k"
echo "-- several disjuncts"
check "SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k OR l.small = r.small"
echo "-- mixed join expression keeps every right row"
check "SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k AND l.v < r.p"
check "SELECT count() FROM l_num AS l SEMI LEFT JOIN r_num AS r ON l.k = r.k AND l.v < r.p"
echo "-- dense small range, converted to a flat array"
check "SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k SETTINGS enable_join_fixed_hash_table_conversion = 1"
echo "-- other join algorithms"
check "SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_algorithm = 'parallel_hash'"
check "SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_algorithm = 'grace_hash'"
check "SELECT k FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_algorithm = 'parallel_hash'"
echo "-- another algorithm may reclaim the right blocks, so they are kept"
check "SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS max_bytes_before_external_join = 1000000"
echo "-- nothing may reclaim them, so they are dropped as they are built"
check "SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, join_algorithm = 'hash'"
check "SELECT k FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, join_algorithm = 'hash'"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE l_num;
    DROP TABLE r_num;
    DROP TABLE r_empty;
"

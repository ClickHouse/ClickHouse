#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

QUERIES=$(cat <<'EOF'
SELECT '-- ANTI, key only';
SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k;
SELECT '-- ANTI, right column selected: always default';
SELECT k, p FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k;
SELECT '-- SEMI, key only';
SELECT k FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k;
SELECT '-- SEMI, right column selected';
SELECT k, p FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k;
SELECT '-- ANTI, right column selected, join_use_nulls';
SELECT k, p FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_use_nulls = 1;
SELECT '-- ANTI/SEMI with ON';
SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k;
SELECT count() FROM l_num AS l SEMI LEFT JOIN r_num AS r ON l.k = r.k;
SELECT '-- one-byte key';
SELECT small FROM l_num ANTI LEFT JOIN r_num USING (small) ORDER BY small;
SELECT small FROM l_num SEMI LEFT JOIN r_num USING (small) ORDER BY small;
SELECT '-- string key';
SELECT s FROM l_num ANTI LEFT JOIN r_num USING (s) ORDER BY s;
SELECT '-- LowCardinality key';
SELECT lc FROM l_num ANTI LEFT JOIN r_num USING (lc) ORDER BY lc;
SELECT lc FROM l_num SEMI LEFT JOIN r_num USING (lc) ORDER BY lc;
SELECT '-- nullable key';
SELECT n FROM l_num ANTI LEFT JOIN r_num USING (n) ORDER BY n;
SELECT n FROM l_num SEMI LEFT JOIN r_num USING (n) ORDER BY n;
SELECT '-- composite key';
SELECT k, s FROM l_num ANTI LEFT JOIN r_num USING (k, s) ORDER BY k;
SELECT '-- empty right table';
SELECT k FROM l_num ANTI LEFT JOIN r_empty USING (k) ORDER BY k;
SELECT k FROM l_num SEMI LEFT JOIN r_empty USING (k) ORDER BY k;
SELECT '-- several disjuncts';
SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k OR l.small = r.small;
SELECT count() FROM l_num AS l SEMI LEFT JOIN r_num AS r ON l.k = r.k OR l.s = r.s;
SELECT l.k, r.p FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k OR l.n = r.n ORDER BY l.k;
SELECT '-- mixed join expression keeps every right row';
SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k AND l.v < r.p SETTINGS enable_analyzer = 1;
SELECT count() FROM l_num AS l SEMI LEFT JOIN r_num AS r ON l.k = r.k AND l.v < r.p SETTINGS enable_analyzer = 1;
SELECT '-- dense small range, converted to a flat array';
SELECT count() FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k SETTINGS enable_join_fixed_hash_table_conversion = 1;
SELECT '-- other join algorithms';
SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_algorithm = 'parallel_hash';
SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_algorithm = 'grace_hash';
SELECT k FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS join_algorithm = 'parallel_hash';
SELECT '-- another algorithm may reclaim the right blocks, so they are kept';
SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS max_bytes_before_external_join = 1000000;
SELECT '-- nothing may reclaim them, so they are dropped as they are built';
SELECT k FROM l_num ANTI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, join_algorithm = 'hash';
SELECT k FROM l_num SEMI LEFT JOIN r_num USING (k) ORDER BY k SETTINGS max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, join_algorithm = 'hash';
SELECT '-- the same with several disjuncts, where the per-row used flags used to keep them';
SELECT l.k, r.p FROM l_num AS l ANTI LEFT JOIN r_num AS r ON l.k = r.k OR l.s = r.s ORDER BY l.k SETTINGS max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, join_algorithm = 'hash';
EOF
)

# `LEFT ANTI` and `LEFT SEMI` without selected right columns run on hash tables that store the keys
# alone. Every query above must return the same thing whether or not those tables are used.
with_key_only=$($CLICKHOUSE_CLIENT --enable_join_key_only_hash_tables=1 --query "$QUERIES")
with_mapped=$($CLICKHOUSE_CLIENT --enable_join_key_only_hash_tables=0 --query "$QUERIES")

if [ "$with_key_only" == "$with_mapped" ]
then
    echo "$with_key_only"
else
    echo "MISMATCH between key-only and mapped hash tables"
    diff <(echo "$with_key_only") <(echo "$with_mapped")
fi

$CLICKHOUSE_CLIENT --query "
    DROP TABLE l_num;
    DROP TABLE r_num;
    DROP TABLE r_empty;
"

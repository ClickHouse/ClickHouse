#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The partition value index must never exclude a partition that can match the filter: the results of the queries
# with and without the index must be equal, for every partition key type and filter shape. The index is used for
# any number of parts here (`partition_value_index_min_parts = 0`). The tables have few partitions (at most 60),
# because creating many parts makes the test too slow in CI.

PARTITION_KEYS=(
    "k1"
    "(k1, toYYYYMMDD(ts))"
    "i32"
    "d"
    "toMonday(d)"
    "s"
    "(s, k1)"
    "n"
    "lc"
    "u"
    "ip"
    "(e, b)"
    "k1 % 16"
)

QUERIES=$(cat <<'EOF'
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 = 7;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 IN (1, 2, 11);
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 NOT IN (1, 2, 11);
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 BETWEEN 4 AND 8;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 > 9 OR k1 < 3;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE NOT (k1 >= 5);
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 = 7 AND ts >= toDateTime('2026-01-02 00:00:00', 'UTC') AND ts < toDateTime('2026-01-04 00:00:00', 'UTC');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE toYYYYMMDD(ts) = 20260103;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE ts >= toDateTime('2026-01-04 00:00:00', 'UTC');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE toDate(ts) = '2026-01-03';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE (k1 = 1 AND toYYYYMMDD(ts) = 20260102) OR (k1 = 2 AND toYYYYMMDD(ts) = 20260103);
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE i32 = -5;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE i32 < -3 OR i32 > 3;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE d = '2026-01-08';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE d >= '2026-01-12';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE d BETWEEN '2026-01-05' AND '2026-01-07';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE toMonday(d) = '2026-01-12';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE s = 'tenant_7';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE s IN ('tenant_1', 'tenant_10', 'tenant_100');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE s LIKE 'tenant_1%';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE s > 'tenant_4';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE s = 'tenant_7' AND k1 = 7;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE s = 'tenant_7' AND k1 = 8;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE n IS NULL;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE n IS NOT NULL AND n < 5;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE n = 3;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE n > 7 OR n IS NULL;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE lc = '5';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE lc IN ('1', '2');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE u = toUUID('00000000-0000-0000-0000-000000000007');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE u > toUUID('00000000-0000-0000-0000-000000000005');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE ip = toIPv4('10.0.0.3');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE ip > toIPv4('10.0.0.5');
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE e = 'b';
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE e != 'a' AND b;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE NOT b;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 % 16 = 3;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE cityHash64(k1) % 4 = 1;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE f = 2.5;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE f > 4;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE isNaN(f);
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 IN (SELECT number FROM numbers(3));
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 = 1000;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE k1 = 1 AND k1 = 2;
SELECT count(), sum(cityHash64(v)) FROM {table} WHERE v < 100;
SELECT count(), sum(cityHash64(v)) FROM {table};
EOF
)

for i in "${!PARTITION_KEYS[@]}"
do
    table="t_partition_value_index_$i"
    partition_key="${PARTITION_KEYS[$i]}"

    # Two inserts, so that many partitions consist of two parts.
    insert_query="
        INSERT INTO $table SELECT
            number % 12,
            toInt32(number % 10) - 5,
            toDateTime('2026-01-01 00:00:00', 'UTC') + (number % 5) * 86400 + (number % 7) * 3600,
            toDate('2026-01-01') + number % 14,
            concat('tenant_', toString(number % 12)),
            if(number % 13 = 0, NULL, number % 10),
            toString(number % 8),
            toUUID(concat('00000000-0000-0000-0000-0000000000', leftPad(toString(number % 8), 2, '0'))),
            toIPv4(concat('10.0.0.', toString(number % 8))),
            CAST(number % 3 + 1, 'Enum8(\\'a\\' = 1, \\'b\\' = 2, \\'c\\' = 3)'),
            number % 2 = 1,
            if(number % 97 = 0, nan, (number % 20) / 4),
            number
        FROM numbers"

    $CLICKHOUSE_CLIENT --max_partitions_per_insert_block 0 -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table
        (
            k1 UInt16,
            i32 Int32,
            ts DateTime('UTC'),
            d Date,
            s String,
            n Nullable(UInt32),
            lc LowCardinality(String),
            u UUID,
            ip IPv4,
            e Enum8('a' = 1, 'b' = 2, 'c' = 3),
            b Bool,
            f Float64,
            v UInt64
        )
        ENGINE = MergeTree
        PARTITION BY $partition_key
        ORDER BY v
        SETTINGS allow_nullable_key = 1;

        SYSTEM STOP MERGES $table;
        $insert_query(0, 600);
        $insert_query(600, 600);"

    table_queries="${QUERIES//\{table\}/$table}"
    with_index=$($CLICKHOUSE_CLIENT --use_partition_value_index 1 --partition_value_index_min_parts 0 -q "$table_queries")
    without_index=$($CLICKHOUSE_CLIENT --use_partition_value_index 0 -q "$table_queries")

    if [ "$with_index" == "$without_index" ]
    then
        echo "PARTITION BY $partition_key: OK"
    else
        echo "PARTITION BY $partition_key: results differ"
        diff <(echo "$without_index") <(echo "$with_index")
    fi

    $CLICKHOUSE_CLIENT -q "DROP TABLE $table"
done

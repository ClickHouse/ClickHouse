#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS index_corruption"

${CLICKHOUSE_CLIENT} -n -q "CREATE TABLE IF NOT EXISTS index_corruption
(
    1F740 LowCardinality(String) CODEC(ZSTD(1)),
    B7E44AC String CODEC(ZSTD(1)),
    3F0ED7DB LowCardinality(String) CODEC(ZSTD(1)),
    CCD44A405D Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    38C3868AC DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    ABC0C7B_EA Nullable(String) MATERIALIZED CCD44A405D['ABC0C7B_EA'],
    7AF String MATERIALIZED CCD44A405D['7AF'],
    21DB_EA String MATERIALIZED CCD44A405D['21DB_EA'],
    4482745CA_EF39713558 Nullable(String) MATERIALIZED CCD44A405D['4482745CA.EF39713558'],
    ABC0C7B_938F19 LowCardinality(String) MATERIALIZED CCD44A405D['B7E44AC_618D.6726ED6AA465A'],
    CCC98F_2221 String MATERIALIZED CCD44A405D['CCC98F_2221'],
    INDEX idx_836_ADE2_72AB mapValues(CCD44A405D) TYPE bloom_filter(0.05) GRANULARITY 1,
    INDEX idx_0989 tokens(lower(B7E44AC)) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_7AF 7AF TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY (toStartOfMinute(38C3868AC), toUnixTimestamp(38C3868AC))
SETTINGS min_rows_for_wide_part = 10000, materialize_ttl_recalculate_only = 1, index_granularity = 8192;"

# This data file contains random data which lead to index marks mismatch after mutation. It happens
# because during mutation we rewrite compact part completely. And surprisingly it may lead to a little bit different
# representation of data on disk, because we can add small last block to the last mark instead of adding new one.
# So rewritten data part may have 1 less mark, than original one. However skip indices were not rewritten during mutation,
# so they still contain marks for original data part. And when we try to read mutated part with skip indices,
# we can "Too many marks" error. This test is added for patch which forces indices recalculation when we rewrite compact part.
${CLICKHOUSE_CLIENT} -q 'insert into index_corruption FORMAT TSVWithNamesAndTypes' < $CURDIR/data_zstd/test_03755.tsv.zst

${CLICKHOUSE_CLIENT} -q 'ALTER TABLE index_corruption (MODIFY TTL toDateTime(38C3868AC) + toIntervalYear(140));'

${CLICKHOUSE_CLIENT} -q "SELECT * FROM index_corruption WHERE (7AF = 'xyz');"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS index_corruption"

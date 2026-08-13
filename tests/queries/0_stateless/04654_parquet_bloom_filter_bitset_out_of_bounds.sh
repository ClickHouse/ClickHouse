#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_parquet/04654_bloom_filter_bitset_out_of_bounds.parquet

# The `s` column's BloomFilterHeader in this file claims a 1 GiB bitset, while its column metadata
# declares only 272 bytes of bloom filter data. Deriving bloom filter block byte ranges from the
# claimed size used to read far outside the buffer holding the bloom filter.
${CLICKHOUSE_LOCAL} --query="SELECT count() FROM file('$DATA_FILE') WHERE s = '42'
    SETTINGS input_format_parquet_bloom_filter_push_down = 1" 2>&1 | grep -c "INCORRECT_DATA"

# The bloom filter is only read when it can prune, so the same file reads fine otherwise.
${CLICKHOUSE_LOCAL} --query="SELECT count() FROM file('$DATA_FILE') WHERE s = '42'
    SETTINGS input_format_parquet_bloom_filter_push_down = 0"
${CLICKHOUSE_LOCAL} --query="SELECT sum(n) FROM file('$DATA_FILE')"

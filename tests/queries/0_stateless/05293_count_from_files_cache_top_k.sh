#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Parquet top-k filtering needs the native reader.

# `ORDER BY ... LIMIT` installs `FormatFilterInfo::top_k_filter`, which drops rows inside the
# Parquet reader. The count-from-files cache key is file identity only, so that reduced count
# must not be stored for a later `count()`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_05293;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_05293 (k UInt64) ENGINE = File(Parquet);
INSERT INTO ${CLICKHOUSE_DATABASE}.t_05293
SELECT number FROM numbers(100000)
SETTINGS output_format_parquet_row_group_size = 10000;
"

$CLICKHOUSE_CLIENT --format JSON -q "
SELECT k FROM ${CLICKHOUSE_DATABASE}.t_05293 ORDER BY k LIMIT 1
SETTINGS
    use_cache_for_count_from_files = 1,
    use_top_k_dynamic_filtering = 1,
    input_format_parquet_use_native_reader_v3 = 1,
    input_format_parquet_filter_push_down = 1,
    max_threads = 1,
    max_parsing_threads = 1,
    max_block_size = 65409
" | python3 -c "
import json, sys
d = json.load(sys.stdin)
k = int(d['data'][0]['k'])
rows_read = int(d['statistics']['rows_read'])
print(f'limit_row\t{k}')
print('topk_reduced_rows\t' + ('1' if rows_read < 100000 else '0'))
"

$CLICKHOUSE_CLIENT -q "
SELECT 'count', count()
FROM ${CLICKHOUSE_DATABASE}.t_05293
SETTINGS
    use_cache_for_count_from_files = 1,
    optimize_count_from_files = 1,
    use_top_k_dynamic_filtering = 0;

DROP TABLE ${CLICKHOUSE_DATABASE}.t_05293;
"

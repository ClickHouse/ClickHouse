#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every setting that shapes an inferred type has to be part of the schema cache key, or a schema
# cached under one value gets handed out for another.

DATA_FILE=$CLICKHOUSE_TMP/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        number AS n,
        toString(number) AS s,
        if(number = 1, NULL, (number, toString(number))) AS t
    FROM numbers(3)
    INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex
"

# `SchemaCache::tryGetImpl` drops an entry whose file is not strictly older than it, and both times
# are whole seconds, so a file written in the same second as the inference is never a cache hit.
# Backdating the file keeps the hit below independent of where the second boundary falls.
touch -d '2000-01-01 00:00:00' "$DATA_FILE"

$CLICKHOUSE_LOCAL -m -q "
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 0;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 0, allow_experimental_nullable_tuple_type = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 0, allow_experimental_nullable_tuple_type = 0 FORMAT Null;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1, schema_inference_make_json_columns_nullable = 0 FORMAT Null;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1, schema_inference_make_json_columns_nullable = 1 FORMAT Null;
    SELECT count() FROM system.schema_inference_cache
    WHERE format = 'Vortex' AND additional_format_info LIKE '%schema_inference_make_columns_nullable%';
"

# A written entry is only half of it: the second identical inference has to read it back, or the
# key could be wrong in a way no schema shows.
$CLICKHOUSE_LOCAL -m -q "
    DESC file('$DATA_FILE', 'Vortex') FORMAT Null;
    DESC file('$DATA_FILE', 'Vortex') FORMAT Null;
    SELECT
        'schema cache hits',
        (SELECT sum(value) FROM system.events WHERE event = 'SchemaInferenceCacheSchemaHits') > 0;
"

# `allow_geoparquet_parser` reaches the Vortex reader too, so it has to be part of the key: the two
# values must not share an entry. A fresh process is used so that only these two are in the cache.
$CLICKHOUSE_LOCAL -m -q "
    DESC file('$DATA_FILE', 'Vortex') SETTINGS input_format_parquet_allow_geoparquet_parser = 1 FORMAT Null;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS input_format_parquet_allow_geoparquet_parser = 0 FORMAT Null;
    SELECT 'geoparquet cache keys', count(), countDistinct(additional_format_info)
    FROM system.schema_inference_cache WHERE format = 'Vortex';
"

rm -f "$DATA_FILE"

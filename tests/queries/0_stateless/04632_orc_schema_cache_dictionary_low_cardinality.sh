#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires the ORC input format, which is not built in fasttest.

# The native ORC schema reader infers a dictionary-encoded String column as
# LowCardinality(...) when input_format_orc_dictionary_as_low_cardinality is on, and as a
# plain String otherwise. The setting must be part of the schema-inference cache key, so a
# query that caches the LowCardinality schema must not poison the cache for a later query
# with the setting off, which must still infer the plain-String schema.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=${CLICKHOUSE_TMP}/04632_dict.orc

# Write the dictionary-encoded file in a separate process so the reading process starts with an
# empty schema cache.
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$DATA_FILE', ORC) SELECT toLowCardinality(toString(number % 10)) AS c FROM numbers(100000) SETTINGS output_format_orc_dictionary_key_size_threshold = 0.1, engine_file_truncate_on_insert = 1"

# Force an old mtime so the cached schema stays valid across the two DESCs below. SchemaCache
# invalidates an entry when st_mtime >= registration_time (src/Storages/Cache/SchemaCache.cpp:125),
# and both are second-granularity time_t; without this, a write and the first DESC in the same
# second would drop the entry and the second DESC would re-infer regardless of the cache key,
# passing even when input_format_orc_dictionary_as_low_cardinality is missing from the key.
touch -t 200001010000 "$DATA_FILE"

$CLICKHOUSE_LOCAL --multiquery "
    -- Infer and cache the LowCardinality schema with the setting on.
    DESC file('$DATA_FILE', ORC) SETTINGS input_format_orc_dictionary_as_low_cardinality = 1;
    -- Infer again with the setting off: must NOT reuse the cached schema, must be plain String.
    DESC file('$DATA_FILE', ORC) SETTINGS input_format_orc_dictionary_as_low_cardinality = 0;
" 2>&1 | grep -oE 'LowCardinality\(Nullable\(String\)\)|Nullable\(String\)'

rm -f "$DATA_FILE"

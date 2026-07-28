#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires the ORC input format, which is not built in fasttest.

# Regression test for a schema-inference cache-key bug in the native ORC reader.
# `input_format_orc_skip_columns_with_unsupported_types_in_schema_inference` changes the inferred
# schema: an unsupported ORC `UNION` column is dropped when the setting is on, while otherwise the
# file is rejected with `UNKNOWN_TYPE`. The setting must therefore be part of the schema-inference
# cache key. Otherwise a query that caches the "skipped" schema poisons the cache for a later
# default-settings query, which would then reuse the cached schema instead of throwing.
#
# The data file has one unsupported `UNION` column `u` and one supported `Int32` column `i`.
# Both queries run in a single clickhouse-local process so they share the schema-inference cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/orc_union_type.orc

$CLICKHOUSE_LOCAL --multiquery "
    -- Infer and cache the schema with the unsupported UNION column dropped.
    DESC file('$DATA_FILE', ORC) SETTINGS input_format_orc_skip_columns_with_unsupported_types_in_schema_inference = 1;
    -- Infer again with default settings: must NOT reuse the cached schema and must throw.
    DESC file('$DATA_FILE', ORC);
" 2>&1 | grep -oE 'Nullable\(Int32\)|UNKNOWN_TYPE' | head -2

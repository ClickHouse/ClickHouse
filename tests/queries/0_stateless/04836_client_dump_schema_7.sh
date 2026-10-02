#!/usr/bin/env bash
# Tags: long, no-fasttest, no-darwin
# Tag no-fasttest: Kafka is not in the fast test build.
# Tag long: starts dozens of clickhouse-local instances.
#
# Carrier-specific replay gates: a gate is emitted exactly once when a carrier needs it, and never
# for a schema without one. Alias and canonical spellings of a setting are both accepted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_err.txt"
LOCAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_src"
REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_replay"
DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"

BADSEL_RE='^SET allow_materialized_view_with_bad_select = 1;'
TS_RE='^SET (allow_experimental_time_series_table|enable_time_series_table) = 1;'
KEEPER_RE='^SET (allow_experimental_kafka_offsets_storage_in_keeper|allow_kafka_offsets_storage_in_keeper) = 1;'
HIVE_RE='^SET allow_experimental_object_storage_queue_hive_partitioning = 1;'
PG_RE='^SET (allow_experimental_materialized_postgresql_table|enable_materialized_postgresql_table) = 1;'
URL_RE='^SET (allow_experimental_url_wildcard_from_index_pages|allow_url_wildcard_from_index_pages) = 1;'

# make_dump <setup SQL>: build the schema in a fresh local instance and dump it to DUMP_FILE.
make_dump()
{
    rm -rf "$LOCAL_PATH"
    $CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --multiquery --query "CREATE DATABASE ${DB}; $1"
    $CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --dump-schema="${DB}" > "$DUMP_FILE" 2>"$ERR_FILE"
    rm -rf "$LOCAL_PATH"
}

# replay_local <label> <table-name LIKE pattern>: replay DUMP_FILE into a fresh local instance.
replay_local()
{
    rm -rf "$REPLAY_PATH"
    $CLICKHOUSE_LOCAL --path "$REPLAY_PATH" --multiquery --queries-file "$DUMP_FILE" > /dev/null 2>"$ERR_FILE"
    local rc=$?
    [[ $rc -eq 0 ]] && echo "OK: $1 replayed" || echo "FAIL: $1 replay rejected: $(cat "$ERR_FILE")"
    echo "$1, replayed objects: $($CLICKHOUSE_LOCAL --path "$REPLAY_PATH" --query "SELECT count() FROM system.tables WHERE database = '${DB}' AND name LIKE '$2' AND name NOT LIKE '.inner%'")"
    rm -rf "$REPLAY_PATH"
}

echo '--- a healthy materialized view does not get the bad-select gate ---'
make_dump "
CREATE TABLE ${DB}.src (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${DB}.dst (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW ${DB}.mv_to TO ${DB}.dst AS SELECT x, y FROM ${DB}.src;
CREATE MATERIALIZED VIEW ${DB}.mv_inner ENGINE = MergeTree ORDER BY x AS SELECT x, y FROM ${DB}.src;
CREATE MATERIALIZED VIEW ${DB}.mv_cols (a Int64, b Int64) ENGINE = MergeTree ORDER BY a AS SELECT x AS a, y AS b FROM ${DB}.src;
CREATE MATERIALIZED VIEW ${DB}.mv_numbers (x Int64) ENGINE = Memory AS SELECT x FROM ${DB}.src WHERE x IN (SELECT number FROM numbers(2));
CREATE MATERIALIZED VIEW ${DB}.mv_merge (x Int64) ENGINE = Memory AS SELECT x FROM ${DB}.src WHERE x IN (SELECT x FROM merge('${DB}', '^src\$'));
CREATE MATERIALIZED VIEW ${DB}.mv_values (x Int64) ENGINE = Memory AS SELECT x FROM ${DB}.src WHERE x IN (SELECT x FROM values('x Int64', 1, 2));
CREATE MATERIALIZED VIEW ${DB}.mv_url (x Int64) ENGINE = Memory AS SELECT x FROM ${DB}.src WHERE x IN (SELECT x FROM url('http://127.0.0.1:1/data.csv', CSV, 'x Int64'));
CREATE MATERIALIZED VIEW ${DB}.mv_folded (x Int64) ENGINE = Memory AS SELECT x FROM ${DB}.src WHERE x IN (SELECT number FROM numbers(1 + 1)) AND x IN (SELECT x FROM values('x Int64', 1 + 1));
"
echo "healthy views, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
replay_local 'healthy views' 'mv%'

echo '--- a materialized view accepted under a relaxed check gets the gate exactly once ---'
make_dump "
CREATE TABLE ${DB}.src (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${DB}.dst (x Int64) ENGINE = MergeTree ORDER BY tuple();
SET allow_materialized_view_with_bad_select = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_bad TO ${DB}.dst AS SELECT x, y FROM ${DB}.src;
"
echo "extra output column, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
replay_local 'extra output column' 'mv%'

make_dump "
CREATE TABLE ${DB}.src (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${DB}.dst (x Int64) ENGINE = MergeTree ORDER BY tuple();
SET allow_materialized_view_with_bad_select = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_bad TO ${DB}.dst AS SELECT x FROM ${DB}.src;
DROP TABLE ${DB}.dst;
"
echo "missing TO target, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
replay_local 'missing TO target' 'mv%'

make_dump "
CREATE TABLE ${DB}.src (x Int64) ENGINE = MergeTree ORDER BY tuple();
SET allow_materialized_view_with_bad_select = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_bad (a Int64) ENGINE = MergeTree ORDER BY a AS SELECT x AS a FROM ${DB}.src;
DROP TABLE ${DB}.src;
"
echo "failed analysis, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
replay_local 'failed analysis' 'mv%'

make_dump "
CREATE TABLE ${DB}.src (x Int64) ENGINE = MergeTree ORDER BY tuple();
SET allow_materialized_view_with_bad_select = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_bad (x Int64) ENGINE = Memory AS SELECT x FROM ${DB}.src WHERE x IN (SELECT x FROM merge('${DB}', '^nothing\$'));
"
echo "merge matching no table, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
replay_local 'merge matching no table' 'mv%'

make_dump "
CREATE TABLE ${DB}.src (x Int64) ENGINE = MergeTree ORDER BY tuple();
SET allow_materialized_view_with_bad_select = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_bad (x Int64) ENGINE = Memory AS SELECT x FROM ${DB}.src WHERE x IN (SELECT x FROM url('http://127.0.0.1:1/data.csv', CSV));
"
echo "url without a structure, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
replay_local 'url without a structure' 'mv%'

echo '--- healthy and bad views together get one gate line ---'
make_dump "
CREATE TABLE ${DB}.src (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${DB}.dst_ok (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${DB}.dst_bad (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW ${DB}.mv_ok TO ${DB}.dst_ok AS SELECT x, y FROM ${DB}.src;
SET allow_materialized_view_with_bad_select = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_bad TO ${DB}.dst_bad AS SELECT x, y FROM ${DB}.src;
"
echo "mixed views, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
replay_local 'mixed views' 'mv%'

echo '--- a healthy materialized view dump replays under a bad-select constraint ---'
CONSTRAINT_DB="${DB}_bad_select_constraint"
CONSTRAINT_USER="${DB}_bad_select_user"
CONSTRAINT_PROFILE="${DB}_bad_select_profile"
rm -rf "$LOCAL_PATH"
$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --multiquery --query "
CREATE DATABASE ${CONSTRAINT_DB};
CREATE TABLE ${CONSTRAINT_DB}.src (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${CONSTRAINT_DB}.dst (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_to TO ${CONSTRAINT_DB}.dst AS SELECT x, y FROM ${CONSTRAINT_DB}.src;
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_inner ENGINE = MergeTree ORDER BY x AS SELECT x, y FROM ${CONSTRAINT_DB}.src;
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_numbers (x Int64) ENGINE = Memory AS SELECT x FROM ${CONSTRAINT_DB}.src WHERE x IN (SELECT number FROM numbers(2));
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_merge (x Int64) ENGINE = Memory AS SELECT x FROM ${CONSTRAINT_DB}.src WHERE x IN (SELECT x FROM merge('${CONSTRAINT_DB}', '^src\$'));
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_values (x Int64) ENGINE = Memory AS SELECT x FROM ${CONSTRAINT_DB}.src WHERE x IN (SELECT x FROM values('x Int64', 1, 2));
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_url (x Int64) ENGINE = Memory AS SELECT x FROM ${CONSTRAINT_DB}.src WHERE x IN (SELECT x FROM url('http://127.0.0.1:1/data.csv', CSV, 'x Int64'));
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_folded (x Int64) ENGINE = Memory AS SELECT x FROM ${CONSTRAINT_DB}.src WHERE x IN (SELECT number FROM numbers(1 + 1)) AND x IN (SELECT x FROM values('x Int64', 1 + 1));
"
$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --dump-schema="$CONSTRAINT_DB" > "$DUMP_FILE" 2>"$ERR_FILE"
rm -rf "$LOCAL_PATH"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB};
    DROP USER IF EXISTS ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE IF EXISTS ${CONSTRAINT_PROFILE};
    CREATE SETTINGS PROFILE ${CONSTRAINT_PROFILE} SETTINGS allow_materialized_view_with_bad_select = 0 CONST;
    CREATE USER ${CONSTRAINT_USER} SETTINGS PROFILE '${CONSTRAINT_PROFILE}';
    GRANT ALL ON *.* TO ${CONSTRAINT_USER};
"
$CLICKHOUSE_CLIENT --user "$CONSTRAINT_USER" --multiquery --queries-file "$DUMP_FILE" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: constrained replay succeeded' || echo "FAIL: constrained replay rejected: $(cat "$ERR_FILE")"
echo "constrained replay views present: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = '${CONSTRAINT_DB}' AND name LIKE 'mv%'")"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB} SYNC;
    DROP USER ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE ${CONSTRAINT_PROFILE};
"

echo '--- TimeSeries and Kafka Keeper gates follow their carriers exactly once ---'
make_dump "
SET allow_experimental_time_series_table = 1;
CREATE TABLE ${DB}.ts_a ENGINE = TimeSeries;
CREATE TABLE ${DB}.ts_b ENGINE = TimeSeries;
CREATE TABLE ${DB}.plain_kafka (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow');
"
echo "TimeSeries tables, time-series gate emitted: $(grep -cE "$TS_RE" "$DUMP_FILE")"
echo "TimeSeries tables and plain Kafka, keeper gate emitted: $(grep -cE "$KEEPER_RE" "$DUMP_FILE")"
make_dump "
CREATE TABLE ${DB}.keeper_a (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow') SETTINGS kafka_keeper_path = '', kafka_replica_name = '';
CREATE TABLE ${DB}.keeper_b (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow') SETTINGS kafka_keeper_path = '', kafka_replica_name = '';
"
echo "Kafka with Keeper settings, keeper gate emitted: $(grep -cE "$KEEPER_RE" "$DUMP_FILE")"
echo "Kafka with Keeper settings, time-series gate emitted: $(grep -cE "$TS_RE" "$DUMP_FILE")"

echo '--- a plain mixed schema gets no carrier-specific gate ---'
make_dump "
CREATE TABLE ${DB}.mt (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${DB}.mem (x Int64) ENGINE = Memory;
CREATE TABLE ${DB}.tiny (x Int64) ENGINE = TinyLog;
CREATE TABLE ${DB}.plain_kafka (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow');
CREATE TABLE ${DB}.dst (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW ${DB}.mv_to TO ${DB}.dst AS SELECT x, y FROM ${DB}.mt;
CREATE MATERIALIZED VIEW ${DB}.mv_inner ENGINE = MergeTree ORDER BY x AS SELECT x, y FROM ${DB}.mt;
CREATE VIEW ${DB}.v AS SELECT x FROM ${DB}.mt;
"
echo "plain mixed schema, hive-partitioning gate emitted: $(grep -c "$HIVE_RE" "$DUMP_FILE")"
echo "plain mixed schema, YTsaurus/Paimon/unique-key/nullable-tuple gates emitted: $(grep -cE '^SET (allow_experimental_ytsaurus_table_engine|allow_experimental_paimon_storage_engine|allow_experimental_unique_key|enable_unique_key|allow_experimental_nullable_tuple_type|enable_nullable_tuple_type) = ' "$DUMP_FILE")"
echo "plain mixed schema, bad-select gate emitted: $(grep -c "$BADSEL_RE" "$DUMP_FILE")"
echo "plain mixed schema, recovery gates emitted: $(grep -cE "${TS_RE}|${KEEPER_RE}|${PG_RE}" "$DUMP_FILE")"
echo "plain mixed schema, time-series gate emitted: $(grep -cE "$TS_RE" "$DUMP_FILE")"
echo "plain mixed schema, keeper gate emitted: $(grep -cE "$KEEPER_RE" "$DUMP_FILE")"
echo "plain mixed schema, carrier-derived shared gates emitted: $(grep -cE '^SET (allow_fuzz_query_functions|allow_deprecated_error_prone_window_functions|allow_hyperscan|allow_suspicious_codecs|allow_suspicious_low_cardinality_types|allow_experimental_full_text_index|allow_suspicious_primary_key|allow_experimental_funnel_functions|allow_experimental_nlp_functions|allow_suspicious_fixed_string_types|allow_suspicious_variant_types|allow_suspicious_ttl_expressions|allow_dynamic_type_in_join_keys|allow_deprecated_syntax_for_merge_tree) = ' "$DUMP_FILE")"
replay_local 'plain mixed schema' '%'

echo '--- the shared gates follow their carriers ---'
make_dump "
CREATE TABLE ${DB}.mt (x Int64, s String) ENGINE = MergeTree ORDER BY x;
SET allow_fuzz_query_functions = 1, allow_deprecated_error_prone_window_functions = 1, allow_suspicious_low_cardinality_types = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_fuzz ENGINE = Memory AS SELECT fuzzQuery(s) AS q FROM ${DB}.mt;
CREATE MATERIALIZED VIEW ${DB}.mv_cast ENGINE = Memory AS SELECT CAST(x, 'LowCardinality(Int64)') AS l FROM ${DB}.mt;
CREATE MATERIALIZED VIEW ${DB}.mv_neighbor ENGINE = Memory AS SELECT neighbor(x, 1) AS n FROM ${DB}.mt;
CREATE MATERIALIZED VIEW ${DB}.mv_multi ENGINE = Memory AS SELECT multiMatchAny(s, ['a']) AS m FROM ${DB}.mt;
"
echo "fuzzQuery materialized view, fuzz-functions gate emitted: $(grep -c '^SET allow_fuzz_query_functions = 1;' "$DUMP_FILE")"
echo "neighbor materialized view, error-prone-window gate emitted: $(grep -c '^SET allow_deprecated_error_prone_window_functions = 1;' "$DUMP_FILE")"
echo "multiMatchAny materialized view, hyperscan gate emitted: $(grep -c '^SET allow_hyperscan = 1;' "$DUMP_FILE")"
echo "CAST materialized view, low-cardinality gate emitted: $(grep -c '^SET allow_suspicious_low_cardinality_types = 1;' "$DUMP_FILE")"
replay_local 'function materialized views' '%'
# A plain view keeps its columns, so replay never analyzes its SELECT.
make_dump "
CREATE TABLE ${DB}.mt (x Int64, s String, d Dynamic) ENGINE = MergeTree ORDER BY x;
SET allow_fuzz_query_functions = 1, allow_deprecated_error_prone_window_functions = 1, allow_suspicious_types_in_group_by = 1;
CREATE VIEW ${DB}.v_fuzz AS SELECT fuzzQuery(s) AS q FROM ${DB}.mt;
CREATE VIEW ${DB}.v_neighbor AS SELECT neighbor(x, 1) AS n FROM ${DB}.mt;
CREATE VIEW ${DB}.v_multi AS SELECT multiMatchAny(s, ['a']) AS m FROM ${DB}.mt;
CREATE VIEW ${DB}.v_group AS SELECT d FROM ${DB}.mt GROUP BY d;
CREATE VIEW ${DB}.v_param AS SELECT neighbor(x, 1) AS n FROM ${DB}.mt WHERE x > {p:Int64};
"
echo "plain views, function gates emitted: $(grep -cE '^SET (allow_fuzz_query_functions|allow_deprecated_error_prone_window_functions|allow_hyperscan) = 1;' "$DUMP_FILE")"
echo "plain views, analyzer gates emitted: $(grep -cE '^SET (allow_suspicious_types_in_group_by|allow_suspicious_types_in_order_by|allow_experimental_correlated_subqueries) = 1;' "$DUMP_FILE")"
replay_local 'plain views' '%'
make_dump "
CREATE TABLE ${DB}.src (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW ${DB}.mv_table_function (s String) ENGINE = Memory AS SELECT s FROM ${DB}.src WHERE s IN (SELECT query FROM fuzzQuery('SELECT 1') LIMIT 1);
"
echo "fuzzQuery table function, fuzz-functions gate emitted: $(grep -c '^SET allow_fuzz_query_functions = 1;' "$DUMP_FILE")"
replay_local 'fuzzQuery table function' 'mv%'
make_dump "
CREATE TABLE ${DB}.c (x Int64 CODEC(Delta, LZ4)) ENGINE = MergeTree ORDER BY x;
"
echo "CODEC column, suspicious-codecs gate emitted: $(grep -c '^SET allow_suspicious_codecs = 1;' "$DUMP_FILE")"
replay_local 'CODEC column' '%'
make_dump "
CREATE TABLE ${DB}.lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
"
echo "LowCardinality column, low-cardinality gate emitted: $(grep -c '^SET allow_suspicious_low_cardinality_types = 1;' "$DUMP_FILE")"
replay_local 'LowCardinality column' '%'
make_dump "
SET allow_experimental_full_text_index = 1;
CREATE TABLE ${DB}.ti (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
"
echo "text index, full-text-index gate emitted: $(grep -c '^SET allow_experimental_full_text_index = 1;' "$DUMP_FILE")"
replay_local 'text index' '%'
make_dump "
SET allow_suspicious_primary_key = 1;
CREATE TABLE ${DB}.pk (k SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k;
"
echo "SimpleAggregateFunction key, suspicious-primary-key gate emitted: $(grep -c '^SET allow_suspicious_primary_key = 1;' "$DUMP_FILE")"
replay_local 'SimpleAggregateFunction key' '%'
make_dump "
CREATE TABLE ${DB}.tc (d DateTime, t Time, x Int64 TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY d;
"
echo "Time column and column TTL, time-type gate emitted: $(grep -c '^SET allow_experimental_time_time64_type = 1;' "$DUMP_FILE")"
echo "Time column and column TTL, suspicious-TTL gate emitted: $(grep -c '^SET allow_suspicious_ttl_expressions = 1;' "$DUMP_FILE")"
replay_local 'Time column and column TTL' '%'
make_dump "
CREATE TABLE ${DB}.tt (d DateTime) ENGINE = MergeTree ORDER BY d TTL d + INTERVAL 1 DAY;
"
echo "table TTL, suspicious-TTL gate emitted: $(grep -c '^SET allow_suspicious_ttl_expressions = 1;' "$DUMP_FILE")"
make_dump "
CREATE TABLE ${DB}.named (time UInt32, ttl UInt32) ENGINE = MergeTree ORDER BY time;
"
echo "columns named time and ttl, time-type gate emitted: $(grep -c '^SET allow_experimental_time_time64_type = 1;' "$DUMP_FILE")"
echo "columns named time and ttl, suspicious-TTL gate emitted: $(grep -c '^SET allow_suspicious_ttl_expressions = 1;' "$DUMP_FILE")"
make_dump "
CREATE TABLE ${DB}.vt (v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY tuple();
"
echo "Variant column, suspicious-variant gate emitted: $(grep -c '^SET allow_suspicious_variant_types = 1;' "$DUMP_FILE")"
replay_local 'Variant column' '%'
make_dump "
CREATE TABLE ${DB}.mt (x Int64) ENGINE = MergeTree ORDER BY x;
SET enable_funnel_functions = 1;
CREATE MATERIALIZED VIEW ${DB}.mv_funnel ENGINE = Memory AS SELECT sequenceNextNodeIf('forward', 'head')(toDateTime(x), toString(x), x = 1, x = 1, x > 0) AS n FROM ${DB}.mt;
"
echo "sequenceNextNodeIf view, funnel gate emitted: $(grep -cE '^SET (allow_experimental_funnel_functions|enable_funnel_functions) = 1;' "$DUMP_FILE")"
replay_local 'sequenceNextNodeIf view' '%'
make_dump "
CREATE VIEW ${DB}.u_plain AS SELECT * FROM url('http://127.0.0.1:1/data.csv', CSV, 'x UInt8');
CREATE TABLE ${DB}.u_brace (x UInt8) ENGINE = URL('http://127.0.0.1:1/{a,b}.csv', CSV);
CREATE TABLE ${DB}.u_pipe (x UInt8) ENGINE = URL('http://127.0.0.1:1/a|b.csv', CSV);
"
echo "url objects without a path wildcard, url-wildcard gate emitted: $(grep -cE "$URL_RE" "$DUMP_FILE")"
replay_local 'url objects without a path wildcard' 'u%'
make_dump "
SET allow_url_wildcard_from_index_pages = 1;
CREATE TABLE ${DB}.u_star (x UInt8) ENGINE = URL('http://127.0.0.1:1/*.csv', CSV);
"
echo "url table with a path wildcard, url-wildcard gate emitted: $(grep -cE "$URL_RE" "$DUMP_FILE")"
replay_local 'url table with a path wildcard' 'u%'
make_dump "
CREATE TABLE ${DB}.named_gates (variant UInt32, lowcardinality UInt32, fixedstring UInt32, json UInt32, neighbor UInt32, INDEX i json TYPE minmax) ENGINE = MergeTree ORDER BY variant;
"
echo "column named variant, suspicious-variant gate emitted: $(grep -c '^SET allow_suspicious_variant_types = 1;' "$DUMP_FILE")"
echo "columns named lowcardinality, fixedstring, json and neighbor, their gates emitted: $(grep -cE '^SET (allow_suspicious_low_cardinality_types|allow_suspicious_fixed_string_types|allow_minmax_index_for_json|allow_deprecated_error_prone_window_functions) = 1;' "$DUMP_FILE")"

echo '--- a plain dump replays under every carrier-gate constraint ---'
CONSTRAINT_DB="${DB}_sweep"
CONSTRAINT_USER="${DB}_sweep_user"
CONSTRAINT_PROFILE="${DB}_sweep_profile"
rm -rf "$LOCAL_PATH"
$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --multiquery --query "
CREATE DATABASE ${CONSTRAINT_DB};
CREATE TABLE ${CONSTRAINT_DB}.mt (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${CONSTRAINT_DB}.mem (x Int64) ENGINE = Memory;
CREATE TABLE ${CONSTRAINT_DB}.plain_kafka (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow');
CREATE TABLE ${CONSTRAINT_DB}.dst (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_to TO ${CONSTRAINT_DB}.dst AS SELECT x, y FROM ${CONSTRAINT_DB}.mt;
CREATE VIEW ${CONSTRAINT_DB}.v AS SELECT x FROM ${CONSTRAINT_DB}.mt;
CREATE TABLE ${CONSTRAINT_DB}.named (time UInt32, ttl UInt32, variant UInt32, lowcardinality UInt32, fixedstring UInt32, json UInt32, neighbor UInt32, INDEX i json TYPE minmax) ENGINE = MergeTree ORDER BY time;
CREATE VIEW ${CONSTRAINT_DB}.vj AS SELECT a.x FROM ${CONSTRAINT_DB}.mt AS a JOIN ${CONSTRAINT_DB}.mem AS b ON a.x = b.x;
CREATE TABLE ${CONSTRAINT_DB}.dk (k Dynamic, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW ${CONSTRAINT_DB}.vdk AS SELECT a.v FROM ${CONSTRAINT_DB}.dk AS a JOIN ${CONSTRAINT_DB}.dk AS b ON a.k = b.k;
CREATE TABLE ${CONSTRAINT_DB}.jm (j JSON, INDEX i j TYPE minmax) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_minmax_index_for_json = 1;
CREATE VIEW ${CONSTRAINT_DB}.u_plain AS SELECT * FROM url('http://127.0.0.1:1/data.csv', CSV, 'x UInt8');
SET allow_deprecated_error_prone_window_functions = 1, enable_funnel_functions = 1, allow_url_wildcard_from_index_pages = 1, enable_nullable_tuple_type = 1;
CREATE VIEW ${CONSTRAINT_DB}.vn AS SELECT neighbor(x, 1) AS n FROM ${CONSTRAINT_DB}.mt;
CREATE VIEW ${CONSTRAINT_DB}.vm AS SELECT multiMatchAny(toString(x), ['a']) AS m FROM ${CONSTRAINT_DB}.mt;
CREATE VIEW ${CONSTRAINT_DB}.vf AS SELECT sequenceNextNodeIf('forward', 'head')(toDateTime(x), toString(x), x = 1, x = 1, x > 0) AS f FROM ${CONSTRAINT_DB}.mt;
CREATE VIEW ${CONSTRAINT_DB}.vlc AS SELECT toLowCardinality(x) AS l FROM ${CONSTRAINT_DB}.mt;
CREATE VIEW ${CONSTRAINT_DB}.u_star AS SELECT * FROM url('http://127.0.0.1:1/*.csv', CSV, 'x UInt8');
CREATE VIEW ${CONSTRAINT_DB}.vnt AS SELECT CAST(NULL, 'Nullable(Tuple(a UInt8))') AS t;
CREATE TABLE ${CONSTRAINT_DB}.strings (s String DEFAULT 'time' COMMENT 'variant') ENGINE = MergeTree ORDER BY tuple() COMMENT 'lowcardinality';
CREATE MATERIALIZED VIEW ${CONSTRAINT_DB}.mv_url (x Int64) ENGINE = Memory AS SELECT x FROM ${CONSTRAINT_DB}.mt WHERE x IN (SELECT x FROM url('http://127.0.0.1:1/time/variant.csv', CSV, 'x Int64'));
"
$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --dump-schema="$CONSTRAINT_DB" > "$DUMP_FILE" 2>"$ERR_FILE"
rm -rf "$LOCAL_PATH"
echo "plain dump, join-key/minmax-JSON/suspicious-index/url-wildcard gates emitted: $(grep -cE "^SET (allow_dynamic_type_in_join_keys|allow_minmax_index_for_json|allow_suspicious_indices) = 1;|${URL_RE}" "$DUMP_FILE")"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB};
    DROP USER IF EXISTS ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE IF EXISTS ${CONSTRAINT_PROFILE};
    CREATE SETTINGS PROFILE ${CONSTRAINT_PROFILE} SETTINGS enable_time_time64_type = 0 CONST, allow_experimental_object_storage_queue_hive_partitioning = 0 CONST, allow_materialized_view_with_bad_select = 0 CONST, enable_time_series_table = 0 CONST, allow_kafka_offsets_storage_in_keeper = 0 CONST, enable_materialized_postgresql_table = 0 CONST, enable_funnel_functions = 0 CONST, allow_experimental_nlp_functions = 0 CONST, allow_experimental_hash_functions = 0 CONST, allow_simdjson = 0 CONST, allow_fuzz_query_functions = 0 CONST, allow_hyperscan = 0 CONST, allow_suspicious_codecs = 0 CONST, allow_deprecated_error_prone_window_functions = 0 CONST, allow_suspicious_low_cardinality_types = 0 CONST, allow_suspicious_fixed_string_types = 0 CONST, allow_suspicious_variant_types = 0 CONST, allow_minmax_index_for_json = 0 CONST, allow_suspicious_indices = 0 CONST, allow_url_wildcard_from_index_pages = 0 CONST, allow_suspicious_primary_key = 0 CONST, allow_suspicious_ttl_expressions = 0 CONST, allow_experimental_full_text_index = 0 CONST, allow_dynamic_type_in_join_keys = 0 CONST, enable_unique_key = 0 CONST, allow_experimental_ytsaurus_table_engine = 0 CONST, allow_experimental_paimon_storage_engine = 0 CONST, enable_nullable_tuple_type = 0 CONST;
    CREATE USER ${CONSTRAINT_USER} SETTINGS PROFILE '${CONSTRAINT_PROFILE}';
    GRANT ALL ON *.* TO ${CONSTRAINT_USER};
"
$CLICKHOUSE_CLIENT --user "$CONSTRAINT_USER" --multiquery --queries-file "$DUMP_FILE" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: constrained replay succeeded' || echo "FAIL: constrained replay rejected: $(cat "$ERR_FILE")"
echo "constrained replay tables present: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = '${CONSTRAINT_DB}' AND NOT startsWith(name, '.')")"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB} SYNC;
    DROP USER ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE ${CONSTRAINT_PROFILE};
"
rm -rf "$LOCAL_PATH" "$REPLAY_PATH" "$DUMP_FILE" "$ERR_FILE"

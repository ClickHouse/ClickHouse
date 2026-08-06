#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the FileLog engine.

# A `_database`/`_table` prefilter on `Merge` must keep pruning a child that stamps its own name,
# even when that child is a streaming engine that does not inherit StorageWithCommonVirtualColumns.
# Reading such a child is not merely wasted work: at the default it fails the whole query, and with
# `stream_like_engine_allow_direct_select` it consumes the queue (durably, for FileLog).

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Own directories: FileLog watches the parent directory of its path, so pointing two tables at files
# in the shared user_files dir would make every table watch every other test's files.
DIR_A="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_a"
DIR_B="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_b"
DIR_C="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_c"
DIR_D="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_d"
DIR_E="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_e"
DIR_F="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_f"
DIR_G="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_g"
rm -rf "$DIR_A" "$DIR_B" "$DIR_C" "$DIR_D" "$DIR_E" "$DIR_F" "$DIR_G"
mkdir -p "$DIR_A" "$DIR_B" "$DIR_C" "$DIR_D" "$DIR_E" "$DIR_F" "$DIR_G"
printf '1,1\n2,2\n3,3\n' > "$DIR_A/data.csv"
printf '1,1\n2,2\n3,3\n' > "$DIR_B/data.csv"
printf '1,1\n2,2\n3,3\n' > "$DIR_C/data.csv"
printf '1,1\n2,2\n3,3\n' > "$DIR_D/data.csv"
printf '1,1\n2,2\n3,3\n' > "$DIR_E/data.csv"
printf '1,1\n2,2\n3,3\n' > "$DIR_F/data.csv"
printf '1,1\n2,2\n3,3\n' > "$DIR_G/data.csv"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_mt"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_mt (k UInt8, v UInt8) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t04742_mt SELECT 10, 100"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_filelog_a"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_filelog_a (k UInt8, v UInt8) ENGINE = FileLog('${DIR_A}/', 'CSV')"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_filelog_b"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_filelog_b (k UInt8, v UInt8) ENGINE = FileLog('${DIR_B}/', 'CSV')"
# An Alias over a streaming target: the alias reports neither `isStreamingStorage` nor its target's
# virtual-column declarations, so it must be recognised through the alias hop.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_alias_filelog"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_filelog_c"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_filelog_c (k UInt8, v UInt8) ENGINE = FileLog('${DIR_C}/', 'CSV')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_alias_filelog ENGINE = Alias(currentDatabase(), t04742_filelog_c)"
# A Buffer over a streaming destination: like the alias it reports neither `isStreamingStorage` nor
# its destination's virtual-column declarations, and unlike a nested Merge it prunes nothing itself,
# so it must be recognised through the Buffer hop. Thresholds keep it from flushing during the test.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_buffer_filelog"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_filelog_d"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_filelog_d (k UInt8, v UInt8) ENGINE = FileLog('${DIR_D}/', 'CSV')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_buffer_filelog (k UInt8, v UInt8) ENGINE = Buffer(currentDatabase(), t04742_filelog_d, 1, 3600, 3600, 100000, 1000000, 10000000, 100000000)"
# A composed chain of wrappers: each hop reports neither `isStreamingStorage` nor its target's
# declarations, and the intermediate one is not itself a streaming engine, so resolving only one level
# admits the chain and reads the FileLog at its end. Nothing forbids these compositions.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_buffer_over_buffer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_buffer_inner"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_filelog_e"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_filelog_e (k UInt8, v UInt8) ENGINE = FileLog('${DIR_E}/', 'CSV')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_buffer_inner (k UInt8, v UInt8) ENGINE = Buffer(currentDatabase(), t04742_filelog_e, 1, 3600, 3600, 100000, 1000000, 10000000, 100000000)"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_buffer_over_buffer (k UInt8, v UInt8) ENGINE = Buffer(currentDatabase(), t04742_buffer_inner, 1, 3600, 3600, 100000, 1000000, 10000000, 100000000)"
# A MaterializedView whose target is a streaming engine. Unlike the wrappers above it does inherit its
# target's declarations, but FileLog declares no `_database` at all, so the two-name test fails and the
# view is admitted -- while `readImpl` delegates the read straight into the FileLog.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_mv_to_filelog"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_mv_source"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t04742_filelog_f"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_filelog_f (k UInt8, v UInt8) ENGINE = FileLog('${DIR_F}/', 'CSV')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t04742_mv_source (k UInt8, v UInt8) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW t04742_mv_to_filelog TO t04742_filelog_f AS SELECT k, v FROM t04742_mv_source"
# A lazy proxy over a streaming engine. With `lazy_load_tables = 1` the object attached in the database
# is a `StorageTableProxy`, which reports neither `isStreamingStorage` nor its nested engine's virtual
# columns, so it must be recognised by recursing into the nested storage.
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t04742_lazy_mt (k UInt8, v UInt8) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE_1}.t04742_lazy_mt SELECT 10, 100"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t04742_lazy_filelog (k UInt8, v UInt8) ENGINE = FileLog('${DIR_G}/', 'CSV')"
# Re-attaching is what leaves the proxy in place of the real storage.
${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${CLICKHOUSE_DATABASE_1}"
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${CLICKHOUSE_DATABASE_1}"

echo '-- arm 1: filtering out the streaming child prunes it, so the query succeeds at the default'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_filelog_a)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 0;
"

echo '-- arm 2 (control): without the filter the streaming child IS read, so the query is refused'
# grep -q, not -c: the runner raises the client log level, which repeats the message.
if ${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM merge(currentDatabase(), '^(t04742_mt|t04742_filelog_a)\$')
SETTINGS stream_like_engine_allow_direct_select = 0;
" 2>&1 | grep -q -F 'QUERY_NOT_ALLOWED'; then echo 'QUERY_NOT_ALLOWED'; else echo 'unexpectedly allowed'; fi

echo '-- arm 3: same filter with direct select allowed still returns only the MergeTree rows'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_filelog_b)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 4: and it did not consume the filtered-out queue -- all 3 records are still there'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_b SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 5 (control): arm 4 did consume them, so a second read sees none'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_b SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 6: an Alias over a streaming child is pruned too, so the query succeeds at the default'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_alias_filelog)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 0;
"

echo '-- arm 6 (control): without the filter the aliased streaming child IS read, so it is refused'
if ${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM merge(currentDatabase(), '^(t04742_mt|t04742_alias_filelog)\$')
SETTINGS stream_like_engine_allow_direct_select = 0;
" 2>&1 | grep -q -F 'QUERY_NOT_ALLOWED'; then echo 'QUERY_NOT_ALLOWED'; else echo 'unexpectedly allowed'; fi

echo '-- arm 7: with direct select allowed the filtered read returns only the MergeTree rows'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_alias_filelog)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 8: and it did not consume the aliased queue -- all 3 records are still there'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_alias_filelog SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 9 (control): arm 8 did consume them, so a second read sees none'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_alias_filelog SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 10: a Buffer over a streaming destination is pruned too, so the query succeeds at the default'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_buffer_filelog)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 0;
"

echo '-- arm 10 (control): without the filter the buffered streaming destination IS read, so it is refused'
if ${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM merge(currentDatabase(), '^(t04742_mt|t04742_buffer_filelog)\$')
SETTINGS stream_like_engine_allow_direct_select = 0;
" 2>&1 | grep -q -F 'QUERY_NOT_ALLOWED'; then echo 'QUERY_NOT_ALLOWED'; else echo 'unexpectedly allowed'; fi

echo '-- arm 11: with direct select allowed the filtered read returns only the MergeTree rows'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_buffer_filelog)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 12: and it did not consume the buffered queue -- all 3 records are still there'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_d SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 12 (control): arm 12 did consume them, so a second read sees none'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_d SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 13: a Buffer over a Buffer over a streaming destination is pruned through both hops'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_buffer_over_buffer)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 0;
"

echo '-- arm 13 (control): without the filter the chained streaming destination IS read, so it is refused'
if ${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM merge(currentDatabase(), '^(t04742_mt|t04742_buffer_over_buffer)\$')
SETTINGS stream_like_engine_allow_direct_select = 0;
" 2>&1 | grep -q -F 'QUERY_NOT_ALLOWED'; then echo 'QUERY_NOT_ALLOWED'; else echo 'unexpectedly allowed'; fi

echo '-- arm 14: with direct select allowed the filtered read returns only the MergeTree rows'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_buffer_over_buffer)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 15: and it did not consume the chained queue -- all 3 records are still there'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_e SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 15 (control): arm 15 did consume them, so a second read sees none'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_e SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 16: a MaterializedView whose target is streaming is pruned too'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_mv_to_filelog)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 0;
"

echo '-- arm 16 (control): without the filter the view IS read, delegating into the FileLog, so it is refused'
if ${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM merge(currentDatabase(), '^(t04742_mt|t04742_mv_to_filelog)\$')
SETTINGS stream_like_engine_allow_direct_select = 0;
" 2>&1 | grep -q -F 'QUERY_NOT_ALLOWED'; then echo 'QUERY_NOT_ALLOWED'; else echo 'unexpectedly allowed'; fi

echo '-- arm 17: with direct select allowed the filtered read returns only the MergeTree rows'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge(currentDatabase(), '^(t04742_mt|t04742_mv_to_filelog)\$')
WHERE _table = 't04742_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 18: and it did not consume the view target queue -- all 3 records are still there'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_f SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 18 (control): arm 18 did consume them, so a second read sees none'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t04742_filelog_f SETTINGS stream_like_engine_allow_direct_select = 1;
"

# Must precede the arms: the first query reaching `getSelectedTables` has already run
# `StorageMerge::getQueryProcessingStage`, which resolves every child and so materializes the proxies.
echo '-- arm 19 positive control: the children really are lazy proxies'
${CLICKHOUSE_CLIENT} --query "
SELECT name, engine FROM system.tables
WHERE database = '${CLICKHOUSE_DATABASE_1}' ORDER BY name;
"

echo '-- arm 19: a lazily-proxied streaming child is pruned too, so the query succeeds at the default'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge('${CLICKHOUSE_DATABASE_1}', '^t04742_lazy_(mt|filelog)\$')
WHERE _table = 't04742_lazy_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 0;
"

echo '-- arm 19 (control): without the filter the proxied streaming child IS read, so it is refused'
if ${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM merge('${CLICKHOUSE_DATABASE_1}', '^t04742_lazy_(mt|filelog)\$')
SETTINGS stream_like_engine_allow_direct_select = 0;
" 2>&1 | grep -q -F 'QUERY_NOT_ALLOWED'; then echo 'QUERY_NOT_ALLOWED'; else echo 'unexpectedly allowed'; fi

echo '-- arm 20: with direct select allowed the filtered read returns only the MergeTree rows'
${CLICKHOUSE_CLIENT} --query "
SELECT k, v FROM merge('${CLICKHOUSE_DATABASE_1}', '^t04742_lazy_(mt|filelog)\$')
WHERE _table = 't04742_lazy_mt' ORDER BY k
SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 21: and it did not consume the proxied queue -- all 3 records are still there'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM ${CLICKHOUSE_DATABASE_1}.t04742_lazy_filelog SETTINGS stream_like_engine_allow_direct_select = 1;
"

echo '-- arm 21 (control): arm 21 did consume them, so a second read sees none'
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM ${CLICKHOUSE_DATABASE_1}.t04742_lazy_filelog SETTINGS stream_like_engine_allow_direct_select = 1;
"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE_1}"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE t04742_mv_to_filelog;
DROP TABLE t04742_mv_source;
DROP TABLE t04742_buffer_over_buffer;
DROP TABLE t04742_buffer_inner;
DROP TABLE t04742_filelog_e;
DROP TABLE t04742_filelog_f;
DROP TABLE t04742_buffer_filelog;
DROP TABLE t04742_alias_filelog;
DROP TABLE t04742_filelog_a;
DROP TABLE t04742_filelog_b;
DROP TABLE t04742_filelog_c;
DROP TABLE t04742_filelog_d;
DROP TABLE t04742_mt;
"

rm -rf "$DIR_A" "$DIR_B" "$DIR_C" "$DIR_D" "$DIR_E" "$DIR_F" "$DIR_G"

#!/usr/bin/env bash
# Tags: long, no-darwin
# Third of three 04836_client_dump_schema files; uses many local instances.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_err.txt"

DB="${CLICKHOUSE_DATABASE}"
DB2="${CLICKHOUSE_DATABASE}_second"

echo '--- a database-less TimeSeries target keeps the session database it was created under ---'
# A TimeSeries target created under USE ${DB2} must retain that qualified external edge.
TS_UNQUAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_ts_unqual"
rm -rf "$TS_UNQUAL_PATH"
$CLICKHOUSE_LOCAL --path "$TS_UNQUAL_PATH" --multiquery "
    CREATE DATABASE ${DB};
    CREATE DATABASE ${DB2};
    USE ${DB2};
    SET allow_experimental_time_series_table = 1;
    CREATE TABLE zzz_ts_metrics (metric_family String, type String, unit String, help String)
        ENGINE = ReplacingMergeTree ORDER BY metric_family;
    CREATE TABLE ${DB}.aaa_ts ENGINE = TimeSeries METRICS zzz_ts_metrics;
"
TS_UNQUAL_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_ts_unqual_dump.sql"
$CLICKHOUSE_LOCAL --path "$TS_UNQUAL_PATH" --dump-schema="${DB}" > "$TS_UNQUAL_DUMP_FILE" 2>"$ERR_FILE"
echo "target named in the creator's database: $(grep -c "${DB}\.aaa_ts depends on ${DB2}\.zzz_ts_metrics" "$ERR_FILE")"
echo "target rebound to the object's own database: $(grep -c "depends on ${DB}\.zzz_ts_metrics" "$ERR_FILE")"
# Dumping both databases makes the cross-database edge orderable, so the target must come first.
$CLICKHOUSE_LOCAL --path "$TS_UNQUAL_PATH" --dump-schema="${DB},${DB2}" > "$TS_UNQUAL_DUMP_FILE" 2>"$ERR_FILE"
TS_U_TARGET_LINE=$(grep -n "CREATE TABLE ${DB2}\.zzz_ts_metrics" "$TS_UNQUAL_DUMP_FILE" | head -1 | cut -d: -f1)
TS_U_LINE=$(grep -n "CREATE TABLE ${DB}\.aaa_ts " "$TS_UNQUAL_DUMP_FILE" | head -1 | cut -d: -f1)
if [ -n "$TS_U_TARGET_LINE" ] && [ -n "$TS_U_LINE" ] && [ "$TS_U_TARGET_LINE" -lt "$TS_U_LINE" ]; then
    echo 'OK: cross-database TimeSeries target still ordered first'
else
    echo "FAIL: cross-database TimeSeries target ordering (target=$TS_U_TARGET_LINE ts=$TS_U_LINE)"
fi
rm -rf "$TS_UNQUAL_PATH" "$TS_UNQUAL_DUMP_FILE"


echo '--- merge(REGEXP(...)) reaching an omitted database is reported ---'
# The regexp can match databases the dump leaves out, and `merge()` still infers its structure from
# them on replay, so that omission has to be reported the way an explicitly named one already is.
MERGE_RE_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_merge_re"
rm -rf "$MERGE_RE_PATH"
$CLICKHOUSE_LOCAL --path "$MERGE_RE_PATH" --multiquery "
    CREATE DATABASE ${DB};
    CREATE DATABASE ${DB2};
    CREATE TABLE ${DB2}.zzz_source (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${DB}.yyy_local (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE MATERIALIZED VIEW ${DB}.aaa_mv ENGINE = MergeTree ORDER BY id AS
        SELECT s.id FROM ${DB}.yyy_local AS s
        LEFT JOIN merge(REGEXP('^${DB2}\$'), '^zzz_source\$') AS m ON s.id = m.id;
"
$CLICKHOUSE_LOCAL --path "$MERGE_RE_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"
echo "omitted regexp-matched dependency named: $(grep -c "${DB}\.aaa_mv depends on ${DB2}\." "$ERR_FILE")"
# Dumping both leaves nothing outside the set, so the same schema reports nothing.
$CLICKHOUSE_LOCAL --path "$MERGE_RE_PATH" --dump-schema="${DB},${DB2}" > /dev/null 2>"$ERR_FILE"
echo "no warning when the regexp stays inside the dump: $(grep -c 'will not be created by this dump' "$ERR_FILE")"
rm -rf "$MERGE_RE_PATH"

echo '--- IN right-hand sides: literals and aliases are not dependencies, qualified tables are ---'
IN_REF_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_in_ref"
rm -rf "$IN_REF_PATH"
$CLICKHOUSE_LOCAL --path "$IN_REF_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.zzz_in_source (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB}.zzz_in_set (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_in_literal AS SELECT * FROM ${DB}.zzz_in_source WHERE val IN ('abc');
CREATE VIEW ${DB}.aaa_in_alias AS WITH tuple(1, 2, 3) AS ev SELECT * FROM ${DB}.zzz_in_source WHERE id IN ev;
CREATE VIEW ${DB}.aaa_in_table AS SELECT * FROM ${DB}.zzz_in_source WHERE id IN ${DB}.zzz_in_set;
"
IN_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_in_dump.sql"
$CLICKHOUSE_LOCAL --path "$IN_REF_PATH" --dump-schema="${DB}" > "$IN_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: zero exit code' || echo 'FAIL: expected zero exit code'
echo "stderr lines: $(wc -l < "$ERR_FILE" | tr -d ' ')"
in_set_line=$(grep -n "CREATE TABLE ${DB}\.zzz_in_set " "$IN_DUMP_FILE" | cut -d: -f1)
in_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_in_table " "$IN_DUMP_FILE" | cut -d: -f1)
if [[ -n "$in_set_line" && -n "$in_view_line" && "$in_set_line" -lt "$in_view_line" ]]; then
    echo 'OK: qualified IN table precedes its reader'
else
    echo 'FAIL: qualified IN dependency not ordered'
fi
rm -rf "$IN_REF_PATH" "$IN_DUMP_FILE"

echo '--- a view reading a name-based helper table replays through the owning object ---'
# In an Ordinary database the MV's inner table keeps the deterministic `.inner.<mv name>` name, so
# the reference replays; the edge must be remapped onto the MV or the view is emitted first.
HELPER_REF_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_helper_ref"
rm -rf "$HELPER_REF_PATH"
$CLICKHOUSE_LOCAL --path "$HELPER_REF_PATH" --multiquery --query "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${DB} ENGINE = Ordinary;
CREATE TABLE ${DB}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.zzz_mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${DB}.src;
CREATE VIEW ${DB}.aaa_view AS SELECT * FROM ${DB}.\`.inner.zzz_mv\`;
"
HELPER_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_helper_dump.sql"
$CLICKHOUSE_LOCAL --path "$HELPER_REF_PATH" --dump-schema="${DB}" > "$HELPER_DUMP_FILE" 2>"$ERR_FILE"
helper_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.zzz_mv " "$HELPER_DUMP_FILE" | cut -d: -f1)
helper_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_view " "$HELPER_DUMP_FILE" | cut -d: -f1)
if [[ -n "$helper_mv_line" && -n "$helper_view_line" && "$helper_mv_line" -lt "$helper_view_line" ]]; then
    echo 'OK: helper-table reader ordered after the owning materialized view'
else
    echo "FAIL: helper-table reader misordered (mv=$helper_mv_line view=$helper_view_line)"
fi
HELPER_REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_helper_replay"
rm -rf "$HELPER_REPLAY_PATH"
$CLICKHOUSE_LOCAL --path "$HELPER_REPLAY_PATH" --allow_deprecated_database_ordinary=1 --queries-file "$HELPER_DUMP_FILE"
echo "replayed view resolves the helper: $($CLICKHOUSE_LOCAL --path "$HELPER_REPLAY_PATH" --query "SELECT count() FROM ${DB}.aaa_view")"
echo "helper emitted as a standalone table: $(grep -c "CREATE TABLE ${DB}\.\`\.inner\.zzz_mv\`" "$HELPER_DUMP_FILE")"
rm -rf "$HELPER_REF_PATH" "$HELPER_REPLAY_PATH" "$HELPER_DUMP_FILE"

echo '--- a view reading a UUID-named helper table is refused ---'
# `.inner_id.<uuid>` embeds a UUID the replayed materialized view will not reuse, so no ordering
# makes that reference replayable; the dump must refuse instead of emitting a broken schema.
HELPER_UUID_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_helper_uuid"
rm -rf "$HELPER_UUID_PATH"
$CLICKHOUSE_LOCAL --path "$HELPER_UUID_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.zzz_mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${DB}.src;
"
INNER_NAME=$($CLICKHOUSE_LOCAL --path "$HELPER_UUID_PATH" --query "SELECT target_table FROM system.tables WHERE database = '${DB}' AND name = 'zzz_mv'")
$CLICKHOUSE_LOCAL --path "$HELPER_UUID_PATH" --query "CREATE VIEW ${DB}.aaa_view AS SELECT * FROM ${DB}.\`${INNER_NAME}\`"
$CLICKHOUSE_LOCAL --path "$HELPER_UUID_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: nonzero exit code' || echo 'FAIL: expected nonzero exit code'
echo "refusal names the owning object: $(grep -c "generated inner storage of ${DB}\.zzz_mv" "$ERR_FILE")"
rm -rf "$HELPER_UUID_PATH"

echo '--- a user table named like a nil-UUID tmp helper is kept ---'
# In a database without UUIDs every materialized view reports the nil UUID, which proves nothing:
# a table literally named .tmp.inner_id.<nil> must not be classified as leftover MV storage.
NILUUID_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_niluuid"
rm -rf "$NILUUID_PATH"
$CLICKHOUSE_LOCAL --path "$NILUUID_PATH" --multiquery --query "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${DB} ENGINE = Ordinary;
CREATE TABLE ${DB}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.zzz_mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${DB}.src;
CREATE TABLE ${DB}.\`.tmp.inner_id.00000000-0000-0000-0000-000000000000\` (id UInt64) ENGINE = MergeTree ORDER BY id;
"
NILUUID_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_niluuid_dump.sql"
$CLICKHOUSE_LOCAL --path "$NILUUID_PATH" --dump-schema="${DB}" > "$NILUUID_DUMP_FILE" 2>"$ERR_FILE"
echo "nil-uuid-named user table present: $(grep -c "CREATE TABLE ${DB}\.\`\.tmp\.inner_id\.00000000-0000-0000-0000-000000000000\` " "$NILUUID_DUMP_FILE")"
rm -rf "$NILUUID_PATH" "$NILUUID_DUMP_FILE"

echo '--- a lookalike table of an explicitly targeted kind is kept ---'
# With METRICS named explicitly the engine generates no metrics helper, so `.inner.metrics.<name>`
# is an ordinary user table; only the kinds left implicit (samples, tags) are engine-owned.
EXPLICIT_KIND_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_explicit_kind"
rm -rf "$EXPLICIT_KIND_PATH"
$CLICKHOUSE_LOCAL --path "$EXPLICIT_KIND_PATH" --multiquery --query "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${DB} ENGINE = Ordinary;
SET allow_experimental_time_series_table = 1;
CREATE TABLE ${DB}.real_metrics (metric_family String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family;
CREATE TABLE ${DB}.aaa_ts ENGINE = TimeSeries METRICS ${DB}.real_metrics;
CREATE TABLE ${DB}.\`.inner.metrics.aaa_ts\` (x UInt8) ENGINE = Memory;
"
EXPLICIT_KIND_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_explicit_kind_dump.sql"
$CLICKHOUSE_LOCAL --path "$EXPLICIT_KIND_PATH" --dump-schema="${DB}" > "$EXPLICIT_KIND_DUMP_FILE" 2>"$ERR_FILE"
echo "lookalike of the explicit kind present: $(grep -c "CREATE TABLE ${DB}\.\`\.inner\.metrics\.aaa_ts\` " "$EXPLICIT_KIND_DUMP_FILE")"
echo "engine-owned helpers of the implicit kinds in dump: $(grep -c "CREATE TABLE ${DB}\.\`\.inner\.\(samples\|tags\)\.aaa_ts\`" "$EXPLICIT_KIND_DUMP_FILE")"
rm -rf "$EXPLICIT_KIND_PATH" "$EXPLICIT_KIND_DUMP_FILE"

echo '--- a merge() reader over a helper table is ordered after its owner ---'
# merge() resolves its regexp against every table on the server, helper tables included, so the
# edge must be recorded and then remapped onto the materialized view that recreates the helper.
MERGE_HELPER_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_merge_helper"
rm -rf "$MERGE_HELPER_PATH"
$CLICKHOUSE_LOCAL --path "$MERGE_HELPER_PATH" --multiquery --query "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${DB} ENGINE = Ordinary;
CREATE TABLE ${DB}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.zzz_mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${DB}.src;
CREATE VIEW ${DB}.aaa_merge AS SELECT * FROM merge('${DB}', '^[.]inner[.]zzz_mv\$');
"
MERGE_HELPER_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_merge_helper_dump.sql"
$CLICKHOUSE_LOCAL --path "$MERGE_HELPER_PATH" --dump-schema="${DB}" > "$MERGE_HELPER_DUMP_FILE" 2>"$ERR_FILE"
mh_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.zzz_mv " "$MERGE_HELPER_DUMP_FILE" | cut -d: -f1)
mh_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_merge " "$MERGE_HELPER_DUMP_FILE" | cut -d: -f1)
if [[ -n "$mh_mv_line" && -n "$mh_view_line" && "$mh_mv_line" -lt "$mh_view_line" ]]; then
    echo 'OK: merge() helper reader ordered after the owning materialized view'
else
    echo "FAIL: merge() helper ordering (mv=$mh_mv_line view=$mh_view_line)"
fi
rm -rf "$MERGE_HELPER_PATH" "$MERGE_HELPER_DUMP_FILE"

echo '--- a database-less reference outside the dump warns instead of dropping silently ---'
# Missing qualified and unqualified references must produce equivalent warnings.
DBLESS_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dbless"
rm -rf "$DBLESS_PATH"
$CLICKHOUSE_LOCAL --path "$DBLESS_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
CREATE TABLE ${DB2}.dsrc (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY ${DB2}.dd (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB '${DB2}')) LAYOUT(FLAT()) LIFETIME(0);
USE ${DB2};
CREATE VIEW ${DB}.v AS SELECT * FROM dictionary('dd');
"
DBLESS_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dbless_dump.sql"
$CLICKHOUSE_LOCAL --path "$DBLESS_PATH" --dump-schema="${DB}" > "$DBLESS_DUMP_FILE" 2>"$ERR_FILE"
echo "dump still emits the view: $(grep -c "CREATE VIEW ${DB}\.v " "$DBLESS_DUMP_FILE")"
grep -o -m1 'without a database; no dumped database contains it' "$ERR_FILE"
rm -rf "$DBLESS_PATH" "$DBLESS_DUMP_FILE"

echo '--- an object whose credentials came back masked is reported, not silently emitted ---'
# Masked CREATE credentials replay as literal [HIDDEN] values, so the dump must report them.
MASKED_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_masked"
MASKED_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_masked.sql"
rm -rf "$MASKED_PATH"
$CLICKHOUSE_LOCAL --path "$MASKED_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.s3t (a Int64) ENGINE = S3('http://example.com/f.csv', 'AKIAEXAMPLEKEY', 'SuperSecret123', 'CSV');
"
$CLICKHOUSE_LOCAL --path "$MASKED_PATH" --dump-schema="${DB}" > "$MASKED_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: zero exit code' || echo 'FAIL: expected zero exit code'
echo "masked table still dumped: $(grep -c "CREATE TABLE ${DB}\.s3t" "$MASKED_DUMP_FILE")"
echo "masked credential reported: $(grep -c 'credentials masked as \[HIDDEN\]' "$ERR_FILE")"
echo "secret in the dump: $(grep -c 'SuperSecret123' "$MASKED_DUMP_FILE")"
rm -rf "$MASKED_PATH" "$MASKED_DUMP_FILE"

echo '--- a materialized view accepted under a relaxed check replays through the prelude ---'
# Materialized-view target compatibility is rechecked from stored CREATE text on replay.
BADSEL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_badsel"
BADSEL_DST="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_badsel_dst"
BADSEL_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_badsel.sql"
rm -rf "$BADSEL_PATH" "$BADSEL_DST"
$CLICKHOUSE_LOCAL --path "$BADSEL_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.src (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${DB}.dst (x Int64, z Int64) ENGINE = MergeTree ORDER BY tuple();
SET allow_materialized_view_with_bad_select = 1;
CREATE MATERIALIZED VIEW ${DB}.mv TO ${DB}.dst AS SELECT x, y FROM ${DB}.src;
"
$CLICKHOUSE_LOCAL --path "$BADSEL_PATH" --dump-schema="${DB}" > "$BADSEL_DUMP_FILE" 2>"$ERR_FILE"
$CLICKHOUSE_LOCAL --path "$BADSEL_DST" --multiquery --queries-file "$BADSEL_DUMP_FILE" > /dev/null 2>"$ERR_FILE"
echo "bad-select view replayed: $($CLICKHOUSE_LOCAL --path "$BADSEL_DST" --query "SELECT count() FROM system.tables WHERE database = '${DB}' AND name = 'mv'")"
rm -rf "$BADSEL_PATH" "$BADSEL_DST" "$BADSEL_DUMP_FILE"

echo '--- the prelude is scoped to what the dumped statements can reach ---'
# A schema without a materialized view must omit its compatibility gate.
NOMV_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nomv"
NOMV_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nomv.sql"
rm -rf "$NOMV_PATH"
$CLICKHOUSE_LOCAL --path "$NOMV_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.plain (x Int64) ENGINE = MergeTree ORDER BY tuple();
"
$CLICKHOUSE_LOCAL --path "$NOMV_PATH" --dump-schema="${DB}" > "$NOMV_DUMP_FILE" 2>"$ERR_FILE"
echo "no-mv schema, mv gate emitted: $(grep -c 'SET allow_materialized_view_with_bad_select' "$NOMV_DUMP_FILE")"
echo "no-mv schema, ungated gate emitted: $(grep -c 'SET allow_experimental_time_series_table' "$NOMV_DUMP_FILE")"
# Analyzer-only gates are omitted when no stored query text can reach them.
echo "no-mv schema, analyzer gate emitted: $(grep -c 'SET allow_suspicious_types_in_group_by' "$NOMV_DUMP_FILE")"
# Excluding every non-predefined database - clickhouse-local also carries `default` - leaves no
# statement to replay, so there is nothing for a gate to guard and the prelude is dropped whole.
$CLICKHOUSE_LOCAL --path "$NOMV_PATH" --dump-schema --dump-schema-exclude="${DB},default" > "$NOMV_DUMP_FILE" 2>"$ERR_FILE"
echo "empty dump, any SET emitted: $(grep -c '^SET ' "$NOMV_DUMP_FILE")"
rm -rf "$NOMV_PATH" "$NOMV_DUMP_FILE"

# A projection is analyzable query text with no view anywhere: its SELECT is resolved at
# description time, so the analyzer-side gates must come back for it.
PROJ_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_proj"
PROJ_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_proj.sql"
rm -rf "$PROJ_PATH"
$CLICKHOUSE_LOCAL --path "$PROJ_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.with_proj (x Int64, PROJECTION p (SELECT x, count() GROUP BY x)) ENGINE = MergeTree ORDER BY tuple();
"
$CLICKHOUSE_LOCAL --path "$PROJ_PATH" --dump-schema="${DB}" > "$PROJ_DUMP_FILE" 2>"$ERR_FILE"
echo "projection schema, analyzer gate emitted: $(grep -c 'SET allow_suspicious_types_in_group_by' "$PROJ_DUMP_FILE")"
echo "projection schema, dead gates emitted: $(grep -cE 'SET (allow_experimental_window_functions|allow_experimental_hash_functions|allow_simdjson) = ' "$PROJ_DUMP_FILE")"
rm -rf "$PROJ_PATH" "$PROJ_DUMP_FILE"

echo '--- a plain dump replays under an unrelated analyzer constraint ---'
CONSTRAINT_DB="${DB}_constraint"
CONSTRAINT_USER="${DB}_constraint_user"
CONSTRAINT_PROFILE="${DB}_constraint_profile"
CONSTRAINT_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_constraint"
CONSTRAINT_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_constraint.sql"
rm -rf "$CONSTRAINT_PATH"
$CLICKHOUSE_LOCAL --path "$CONSTRAINT_PATH" --multiquery --query "
    CREATE DATABASE ${CONSTRAINT_DB};
    CREATE TABLE ${CONSTRAINT_DB}.plain (x Int64) ENGINE = MergeTree ORDER BY tuple();
"
$CLICKHOUSE_LOCAL --path "$CONSTRAINT_PATH" --dump-schema="$CONSTRAINT_DB" > "$CONSTRAINT_DUMP_FILE" 2>"$ERR_FILE"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB};
    DROP USER IF EXISTS ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE IF EXISTS ${CONSTRAINT_PROFILE};
    CREATE SETTINGS PROFILE ${CONSTRAINT_PROFILE} SETTINGS allow_suspicious_types_in_group_by = 0 CONST;
    CREATE USER ${CONSTRAINT_USER} SETTINGS PROFILE '${CONSTRAINT_PROFILE}';
    GRANT CREATE DATABASE, CREATE TABLE ON *.* TO ${CONSTRAINT_USER};
    GRANT TABLE ENGINE ON * TO ${CONSTRAINT_USER};
"
$CLICKHOUSE_CLIENT --user "$CONSTRAINT_USER" --multiquery --queries-file "$CONSTRAINT_DUMP_FILE" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: constrained replay succeeded' || echo "FAIL: constrained replay rejected: $(cat "$ERR_FILE")"
echo "constrained replay table present: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = '${CONSTRAINT_DB}' AND name = 'plain'")"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB} SYNC;
    DROP USER ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE ${CONSTRAINT_PROFILE};
"
rm -rf "$CONSTRAINT_PATH" "$CONSTRAINT_DUMP_FILE"

echo '--- the prelude keeps the residual shared gates conservative ---'
# Plain tables can still reach type, expression, key, and deprecated-syntax validators.
SHARED_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_shared"
SHARED_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_shared.sql"
rm -rf "$SHARED_PATH"
$CLICKHOUSE_LOCAL --path "$SHARED_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.plain (x Int64) ENGINE = MergeTree ORDER BY tuple();
"
$CLICKHOUSE_LOCAL --path "$SHARED_PATH" --dump-schema="${DB}" > "$SHARED_DUMP_FILE" 2>"$ERR_FILE"
# A stored-DDL gate from the shared list — always emitted.
echo "unique-key gate present: $(grep -c 'SET allow_experimental_unique_key = 1;' "$SHARED_DUMP_FILE")"
# A suspicious-type gate from the shared list — always emitted.
echo "suspicious-primary-key gate present: $(grep -c 'SET allow_suspicious_primary_key = 1;' "$SHARED_DUMP_FILE")"
# A default-expression / function gate from the shared list — always emitted.
echo "fuzz-functions gate present: $(grep -c 'SET allow_fuzz_query_functions = 1;' "$SHARED_DUMP_FILE")"
# A deprecated-syntax gate from the shared list — always emitted.
echo "deprecated-mt-syntax gate present: $(grep -c 'SET allow_deprecated_syntax_for_merge_tree = 1;' "$SHARED_DUMP_FILE")"
rm -rf "$SHARED_PATH" "$SHARED_DUMP_FILE"

echo '--- a dump with a MATERIALIZED expression replays through the prelude ---'
# A MATERIALIZED expression re-validates function gates at replay; the prelude carries them.
MATDEF_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_matdef"
MATDEF_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_matdef.sql"
rm -rf "$MATDEF_PATH"
$CLICKHOUSE_LOCAL --path "$MATDEF_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.with_mat (id UInt64, created_at DateTime MATERIALIZED now()) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_LOCAL --path "$MATDEF_PATH" --dump-schema="${DB}" > "$MATDEF_DUMP_FILE" 2>"$ERR_FILE"
MATDEF_REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_matdef_replay"
rm -rf "$MATDEF_REPLAY_PATH"
$CLICKHOUSE_LOCAL --path "$MATDEF_REPLAY_PATH" --queries-file "$MATDEF_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: replayed into a default session' || echo 'FAIL: replay needed settings the dump did not carry'
echo "replayed MATERIALIZED table present: $($CLICKHOUSE_LOCAL --path "$MATDEF_REPLAY_PATH" --query "SELECT count() FROM system.tables WHERE database = '${DB}' AND name = 'with_mat'")"
rm -rf "$MATDEF_PATH" "$MATDEF_REPLAY_PATH" "$MATDEF_DUMP_FILE"

echo '--- a remote() named collection on a loopback address names a local dependency ---'
# `parseRemoteFunctionArguments` tries named collections before clusters for an identifier first
# argument, so a collection holding a loopback address reads its table from the local catalog. Both
# readers sort before `zzz_nc_src`, so without the edge they would be dumped ahead of their source.
NC_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc"
NC_CONF="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc.xml"
NC_USERS_CONF="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_users.xml"
NC_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_dump.sql"
rm -rf "$NC_PATH"
# Collection values require the server switch, format setting, and user grant together.
cat > "$NC_USERS_CONF" <<EOF
<clickhouse>
    <profiles>
        <default><format_display_secrets_in_show_and_select>1</format_display_secrets_in_show_and_select></default>
    </profiles>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
            <named_collection_control>1</named_collection_control>
            <show_named_collections_secrets>1</show_named_collections_secrets>
        </default>
    </users>
    <quotas><default></default></quotas>
</clickhouse>
EOF
cat > "$NC_CONF" <<EOF
<clickhouse>
    <users_config>${NC_USERS_CONF}</users_config>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
EOF
$CLICKHOUSE_LOCAL --config-file "$NC_CONF" --path "$NC_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE NAMED COLLECTION nc_local AS host = '127.0.0.1', db = '${DB}', table = 'zzz_nc_src';
CREATE TABLE ${DB}.zzz_nc_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_nc_reader AS SELECT * FROM remote(nc_local);
CREATE VIEW ${DB}.aab_nc_reader_override AS SELECT * FROM remote(nc_local, table = 'zzz_nc_src');
"
if $CLICKHOUSE_LOCAL --config-file "$NC_CONF" --path "$NC_PATH" --format_display_secrets_in_show_and_select=1 \
    --dump-schema="${DB}" > "$NC_DUMP_FILE" 2>"$ERR_FILE"; then
    NC_SRC_LINE=$(grep -n "CREATE TABLE ${DB}\.zzz_nc_src" "$NC_DUMP_FILE" | head -1 | cut -d: -f1)
    for reader in aaa_nc_reader aab_nc_reader_override; do
        NC_READER_LINE=$(grep -n "CREATE VIEW ${DB}\.${reader} " "$NC_DUMP_FILE" | head -1 | cut -d: -f1)
        if [ -n "$NC_SRC_LINE" ] && [ -n "$NC_READER_LINE" ] && [ "$NC_SRC_LINE" -lt "$NC_READER_LINE" ]; then
            echo "OK: named-collection source dumped before ${reader}"
        else
            echo "FAIL: named-collection dependency of ${reader} missing or misordered (src=$NC_SRC_LINE reader=$NC_READER_LINE)"
        fi
    done
else
    echo "FAIL: dump rejected: $(cat "$ERR_FILE")"
fi
rm -f "$NC_DUMP_FILE"

echo '--- a remote() named collection is reported, not emitted, and its readers break on replay ---'
# A collection lives on the server rather than in a database, and its values can include credentials,
# so the dump names it in a warning instead of a CREATE NAMED COLLECTION that would print them.
# A stored view carries its column list, so replay creates the reader and only reading it fails.
NC_REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_replay"
rm -rf "$NC_REPLAY_PATH"
$CLICKHOUSE_LOCAL --config-file "$NC_CONF" --path "$NC_PATH" --format_display_secrets_in_show_and_select=1 \
    --dump-schema="${DB}" > "$NC_DUMP_FILE" 2>"$ERR_FILE"
echo "readers warned about the collection: $(grep -c 'depends on named collection nc_local' "$ERR_FILE")"
echo "collection emitted into the dump: $(grep -c 'CREATE NAMED COLLECTION' "$NC_DUMP_FILE")"
echo "collection values leaked into the dump: $(grep -c '127\.0\.0\.1' "$NC_DUMP_FILE")"
$CLICKHOUSE_LOCAL --path "$NC_REPLAY_PATH" --queries-file "$NC_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: schema replayed without the collection' || echo "FAIL: replay rejected: $(cat "$ERR_FILE")"
$CLICKHOUSE_LOCAL --path "$NC_REPLAY_PATH" --query "SELECT count() FROM ${DB}.aaa_nc_reader" > /dev/null 2>"$ERR_FILE"
echo "replayed reader needs the collection: $(grep -c 'nc_local' "$ERR_FILE")"
rm -rf "$NC_REPLAY_PATH"
rm -f "$NC_DUMP_FILE"

echo '--- a remote() named collection the dump session cannot read is refused, not assumed remote ---'
# Same fixture without the secrets config: every value is [HIDDEN], so the address is unknown.
if $CLICKHOUSE_LOCAL --path "$NC_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded over a named collection it could not read'
else
    echo "masked named collection refused: $(grep -c 'values are masked for this session' "$ERR_FILE")"
fi

echo '--- a remote() identifier that names neither a cluster nor a collection is refused ---'
# Dropping the collection leaves the stored views naming something the dump cannot classify.
$CLICKHOUSE_LOCAL --config-file "$NC_CONF" --path "$NC_PATH" --query "DROP NAMED COLLECTION nc_local"
if $CLICKHOUSE_LOCAL --config-file "$NC_CONF" --path "$NC_PATH" --format_display_secrets_in_show_and_select=1 \
    --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded over an unresolvable remote() first argument'
else
    echo "unresolvable remote() first argument refused: $(grep -c 'neither a cluster nor a named collection' "$ERR_FILE")"
fi
rm -rf "$NC_PATH" "$NC_CONF" "$NC_USERS_CONF"

echo '--- a named collection carried by a stored engine or dictionary source is reported ---'
# A collection reaches a persisted CREATE outside a view body too: a table engine keeps it as its
# first argument and a dictionary source as its `NAME` key, and neither is created by the dump.
NCC_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_carrier"
NCC_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc_carrier_dump.sql"
rm -rf "$NCC_PATH"
$CLICKHOUSE_LOCAL --path "$NCC_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE NAMED COLLECTION nc_url AS url = 'http://127.0.0.1:1/', format = 'TSV', structure = 'id UInt64';
CREATE NAMED COLLECTION nc_dict AS host = '127.0.0.1', port = 9000, db = '${DB}', table = 'nc_dict_src';
CREATE TABLE ${DB}.nc_url_reader (id UInt64) ENGINE = URL(nc_url);
CREATE TABLE ${DB}.nc_nested_url_reader (id UInt64) ENGINE = Remote('127.0.0.1:1', url(nc_url));
CREATE TABLE ${DB}.nc_scalar_src (nc_url Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW ${DB}.nc_scalar_reader AS SELECT abs(nc_url) FROM ${DB}.nc_scalar_src;
CREATE DICTIONARY ${DB}.nc_dict_reader (id UInt64, v String) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME nc_dict)) LAYOUT(FLAT()) LIFETIME(0);
"
if $CLICKHOUSE_LOCAL --path "$NCC_PATH" --dump-schema="${DB}" > "$NCC_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "engine carrier warned: $(grep -c 'nc_url_reader depends on named collection nc_url' "$ERR_FILE")"
    echo "nested engine carrier warned: $(grep -c 'nc_nested_url_reader depends on named collection nc_url' "$ERR_FILE")"
    echo "dictionary carrier warned: $(grep -c 'nc_dict_reader depends on named collection nc_dict' "$ERR_FILE")"
    echo "ordinary scalar function warned: $(grep -c 'nc_scalar_reader.*named collection' "$ERR_FILE")"
    echo "collections emitted into the dump: $(grep -c 'CREATE NAMED COLLECTION' "$NCC_DUMP_FILE")"
    echo "engine carrier dumped naming the collection: $(grep -c 'ENGINE = URL(nc_url)' "$NCC_DUMP_FILE")"
else
    echo "FAIL: dump rejected: $(cat "$ERR_FILE")"
fi
rm -rf "$NCC_PATH"
rm -f "$NCC_DUMP_FILE"

echo '--- an RBAC-hidden named collection carrier is reported explicitly ---'
RBAC_DB="${DB}_nc_rbac"
RBAC_USER="${DB}_nc_rbac_user"
RBAC_COLLECTION="${DB}_nc_rbac_collection"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${RBAC_DB};
    DROP USER IF EXISTS ${RBAC_USER};
    DROP NAMED COLLECTION IF EXISTS ${RBAC_COLLECTION};
    CREATE DATABASE ${RBAC_DB};
    CREATE NAMED COLLECTION ${RBAC_COLLECTION} AS url = 'http://127.0.0.1:1/', format = 'TSV';
    CREATE TABLE ${RBAC_DB}.reader (id UInt64) ENGINE = URL(${RBAC_COLLECTION});
    CREATE USER ${RBAC_USER};
    GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON *.* TO ${RBAC_USER};
    GRANT SHOW DATABASES ON *.* TO ${RBAC_USER};
"
if $CLICKHOUSE_CLIENT --user "$RBAC_USER" --dump-schema="$RBAC_DB" > /dev/null 2>"$ERR_FILE"; then
    echo "hidden carrier warned: $(grep -c "reader may depend on named collection ${RBAC_COLLECTION}" "$ERR_FILE")"
else
    echo "FAIL: restricted dump rejected: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP USER ${RBAC_USER};
    DROP DATABASE ${RBAC_DB} SYNC;
    DROP NAMED COLLECTION ${RBAC_COLLECTION};
"
rm -f "$ERR_FILE"

echo '--- Remote databases replay from database DDL, not proxy table DDL ---'
REMOTE_DB="${DB}_remote_a_proxy"
REMOTE_SECURE_DB="${DB}_remote_b_secure_proxy"
REMOTE_READER_DB="${DB}_remote_c_reader"
REMOTE_SOURCE_DB="${DB}_remote_z_source"
REMOTE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_remote_database.sql"
REMOTE_DUMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_remote_database_dir"
REMOTE_DIR_OUTPUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_remote_database_dir.out"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${REMOTE_READER_DB};
    DROP DATABASE IF EXISTS ${REMOTE_SECURE_DB};
    DROP DATABASE IF EXISTS ${REMOTE_DB};
    DROP DATABASE IF EXISTS ${REMOTE_SOURCE_DB};
    CREATE DATABASE ${REMOTE_SOURCE_DB};
    CREATE TABLE ${REMOTE_SOURCE_DB}.visible_table (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '${REMOTE_SOURCE_DB}');
    CREATE DATABASE ${REMOTE_SECURE_DB} ENGINE = RemoteSecure('127.0.0.1:${CLICKHOUSE_PORT_TCP_SECURE}', '${REMOTE_SOURCE_DB}');
    CREATE DATABASE ${REMOTE_READER_DB};
    CREATE VIEW ${REMOTE_READER_DB}.from_remote AS SELECT * FROM ${REMOTE_DB}.visible_table;
    CREATE VIEW ${REMOTE_READER_DB}.from_remote_secure AS SELECT * FROM ${REMOTE_SECURE_DB}.visible_table;
"
if $CLICKHOUSE_CLIENT --show_remote_databases_in_system_tables=0 --dump-schema="${REMOTE_DB}" \
    > "$REMOTE_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "external source warning retained: $(grep -c "${REMOTE_DB}\.visible_table depends on ${REMOTE_SOURCE_DB}\.visible_table" "$ERR_FILE")"
else
    echo "FAIL: remote database dump rejected: $(cat "$ERR_FILE")"
fi

REMOTE_DATABASES="${REMOTE_READER_DB},${REMOTE_SECURE_DB},${REMOTE_DB},${REMOTE_SOURCE_DB}"
if $CLICKHOUSE_CLIENT --show_remote_databases_in_system_tables=0 --dump-schema="$REMOTE_DATABASES" \
    > "$REMOTE_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "Remote proxy table DDL emitted: $(grep -c "CREATE TABLE ${REMOTE_DB}\.visible_table" "$REMOTE_DUMP_FILE")"
    echo "RemoteSecure proxy table DDL emitted: $(grep -c "CREATE TABLE ${REMOTE_SECURE_DB}\.visible_table" "$REMOTE_DUMP_FILE")"
else
    echo "FAIL: combined remote database dump rejected: $(cat "$ERR_FILE")"
fi

rm -rf "$REMOTE_DUMP_DIR"
if $CLICKHOUSE_CLIENT --show_remote_databases_in_system_tables=0 --dump-schema="$REMOTE_DATABASES" \
    --dump-schema-dir="$REMOTE_DUMP_DIR" > "$REMOTE_DIR_OUTPUT" 2>"$ERR_FILE"; then
    REMOTE_LINE=$(grep -n "Dumped database ${REMOTE_DB} schema" "$REMOTE_DIR_OUTPUT" | cut -d: -f1)
    SECURE_LINE=$(grep -n "Dumped database ${REMOTE_SECURE_DB} schema" "$REMOTE_DIR_OUTPUT" | cut -d: -f1)
    READER_LINE=$(grep -n "Dumped database ${REMOTE_READER_DB} schema" "$REMOTE_DIR_OUTPUT" | cut -d: -f1)
    if [ "$REMOTE_LINE" -lt "$READER_LINE" ] && [ "$SECURE_LINE" -lt "$READER_LINE" ]; then
        echo 'OK: directory dump orders proxies before readers'
    else
        echo "FAIL: directory order remote=$REMOTE_LINE secure=$SECURE_LINE reader=$READER_LINE"
    fi
else
    echo "FAIL: remote database directory dump rejected: $(cat "$ERR_FILE")"
fi

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE ${REMOTE_READER_DB};
    DROP DATABASE ${REMOTE_SECURE_DB};
    DROP DATABASE ${REMOTE_DB};
    DROP DATABASE ${REMOTE_SOURCE_DB} SYNC;
"
if $CLICKHOUSE_CLIENT --multiquery --queries-file "$REMOTE_DUMP_FILE" > /dev/null 2>"$ERR_FILE"; then
    echo 'OK: external database-only dump replayed'
    echo "replayed Remote table resolves: $($CLICKHOUSE_CLIENT -q "EXISTS TABLE ${REMOTE_DB}.visible_table")"
    echo "replayed RemoteSecure table resolves: $($CLICKHOUSE_CLIENT -q "EXISTS TABLE ${REMOTE_SECURE_DB}.visible_table")"
    echo "replayed Remote reader exists: $($CLICKHOUSE_CLIENT -q "EXISTS VIEW ${REMOTE_READER_DB}.from_remote")"
    echo "replayed RemoteSecure reader exists: $($CLICKHOUSE_CLIENT -q "EXISTS VIEW ${REMOTE_READER_DB}.from_remote_secure")"
else
    echo "FAIL: remote database dump did not replay: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${REMOTE_READER_DB};
    DROP DATABASE IF EXISTS ${REMOTE_SECURE_DB};
    DROP DATABASE IF EXISTS ${REMOTE_DB};
    DROP DATABASE IF EXISTS ${REMOTE_SOURCE_DB} SYNC;
"
rm -rf "$REMOTE_DUMP_DIR"
rm -f "$REMOTE_DUMP_FILE" "$REMOTE_DIR_OUTPUT" "$ERR_FILE"

echo '--- directory dump orders a Remote proxy before an in-dump source table reader ---'
CYCLE_SRC_DB="${DB}_cycle_src"
CYCLE_PROXY_DB="${DB}_cycle_proxy"
CYCLE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_cycle_dir"
CYCLE_DIR_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_cycle_dir.out"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CYCLE_PROXY_DB};
    DROP DATABASE IF EXISTS ${CYCLE_SRC_DB};
    CREATE DATABASE ${CYCLE_SRC_DB};
    CREATE TABLE ${CYCLE_SRC_DB}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE DATABASE ${CYCLE_PROXY_DB} ENGINE = Remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '${CYCLE_SRC_DB}');
    CREATE VIEW ${CYCLE_SRC_DB}.v AS SELECT * FROM ${CYCLE_PROXY_DB}.t;
"
if $CLICKHOUSE_CLIENT --show_remote_databases_in_system_tables=0 --dump-schema="${CYCLE_PROXY_DB},${CYCLE_SRC_DB}" \
    --dump-schema-dir="$CYCLE_DIR" > "$CYCLE_DIR_OUT" 2>"$ERR_FILE"; then
    PROXY_LINE=$(grep -n "Dumped database ${CYCLE_PROXY_DB} schema" "$CYCLE_DIR_OUT" | cut -d: -f1)
    SRC_LINE=$(grep -n "Dumped database ${CYCLE_SRC_DB} schema" "$CYCLE_DIR_OUT" | cut -d: -f1)
    if [ "$PROXY_LINE" -lt "$SRC_LINE" ]; then
        echo 'OK: proxy ordered before source database'
    else
        echo "FAIL: expected proxy before source: proxy=$PROXY_LINE src=$SRC_LINE"
    fi
else
    echo "FAIL: cycle directory dump rejected: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE ${CYCLE_PROXY_DB};
    DROP DATABASE ${CYCLE_SRC_DB} SYNC;
"
if $CLICKHOUSE_CLIENT --multiquery --queries-file "$CYCLE_DIR/${CYCLE_PROXY_DB}.sql" > /dev/null 2>"$ERR_FILE" \
    && $CLICKHOUSE_CLIENT --multiquery --queries-file "$CYCLE_DIR/${CYCLE_SRC_DB}.sql" > /dev/null 2>"$ERR_FILE"; then
    echo 'OK: replayed cycle directory dump in order'
    echo "replayed view resolves: $($CLICKHOUSE_CLIENT -q "EXISTS VIEW ${CYCLE_SRC_DB}.v")"
else
    echo "FAIL: replay failed: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CYCLE_PROXY_DB};
    DROP DATABASE IF EXISTS ${CYCLE_SRC_DB} SYNC;
"
rm -rf "$CYCLE_DIR" "$CYCLE_DIR_OUT" "$ERR_FILE"

echo '--- a simple dump does not read protected cluster or macro metadata ---'
LEAN_DB="${DB}_lean_rbac"
LEAN_USER="${DB}_lean_rbac_user"
LEAN_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_lean_rbac.sql"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${LEAN_DB};
    DROP USER IF EXISTS ${LEAN_USER};
    CREATE DATABASE ${LEAN_DB};
    CREATE TABLE ${LEAN_DB}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE USER ${LEAN_USER};
    GRANT SHOW DATABASES ON *.* TO ${LEAN_USER};
    GRANT SHOW TABLES, SHOW COLUMNS ON ${LEAN_DB}.* TO ${LEAN_USER};
    GRANT SELECT ON system.columns TO ${LEAN_USER};
    GRANT SELECT ON system.databases TO ${LEAN_USER};
    GRANT SELECT ON system.settings TO ${LEAN_USER};
    GRANT SELECT ON system.tables TO ${LEAN_USER};
"
if $CLICKHOUSE_CLIENT --user "$LEAN_USER" --query "SELECT count() FROM system.clusters" > /dev/null 2>&1; then
    echo 'FAIL: restricted user can read system.clusters'
else
    echo 'OK: system.clusters is denied'
fi
if $CLICKHOUSE_CLIENT --user "$LEAN_USER" --query "SELECT count() FROM system.macros" > /dev/null 2>&1; then
    echo 'FAIL: restricted user can read system.macros'
else
    echo 'OK: system.macros is denied'
fi
if $CLICKHOUSE_CLIENT --user "$LEAN_USER" --dump-schema="${LEAN_DB}" > "$LEAN_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "restricted dump table present: $(grep -c "CREATE TABLE ${LEAN_DB}\.t" "$LEAN_DUMP_FILE")"
else
    echo "FAIL: restricted dump rejected: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT --multiquery --query "DROP USER ${LEAN_USER}; DROP DATABASE ${LEAN_DB} SYNC;"
rm -f "$LEAN_DUMP_FILE" "$ERR_FILE"

echo '--- a same-server hostname in remote() is discovered as a local dependency ---'
# The connected server's own hostname is recognized as a local replica.
SAME_HOST_DB="${DB}_same_host"
SAME_HOST_DUMP="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_same_host.sql"
SERVER_HOST=$($CLICKHOUSE_CLIENT -q "SELECT hostName()")
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${SAME_HOST_DB};
    CREATE DATABASE ${SAME_HOST_DB};
    CREATE TABLE ${SAME_HOST_DB}.zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE VIEW ${SAME_HOST_DB}.aaa_reader AS SELECT * FROM remote('${SERVER_HOST}:${CLICKHOUSE_PORT_TCP}', '${SAME_HOST_DB}', 'zzz_src');
"
if $CLICKHOUSE_CLIENT --dump-schema="${SAME_HOST_DB}" > "$SAME_HOST_DUMP" 2>"$ERR_FILE"; then
    SRC_LINE=$(grep -n "CREATE TABLE ${SAME_HOST_DB}\.zzz_src" "$SAME_HOST_DUMP" | head -1 | cut -d: -f1)
    READER_LINE=$(grep -n "CREATE VIEW ${SAME_HOST_DB}\.aaa_reader" "$SAME_HOST_DUMP" | head -1 | cut -d: -f1)
    if [ -n "$SRC_LINE" ] && [ -n "$READER_LINE" ] && [ "$SRC_LINE" -lt "$READER_LINE" ]; then
        echo 'OK: same-server hostname source dumped before reader'
    else
        echo "FAIL: same-server hostname dependency misordered (src=$SRC_LINE reader=$READER_LINE)"
    fi
else
    echo "FAIL: same-server hostname dump rejected: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${SAME_HOST_DB} SYNC;"
rm -f "$SAME_HOST_DUMP" "$ERR_FILE"


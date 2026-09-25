#!/usr/bin/env bash
# Tags: long, no-darwin, zookeeper
# Uses many local instances, case-distinct databases, and a ReplicatedMergeTree fixture.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Isolated `--path` directories for two clickhouse-local instances (source, and a fresh one to
# replay the dump into), plus scratch files for dumped SQL / errors / confirmation output.
SRC_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_src"
DST_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dst"
DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dump.sql"
ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_err.txt"
DUMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dir"

DB="${CLICKHOUSE_DATABASE}"
DB2="${CLICKHOUSE_DATABASE}_second"
DB3="${CLICKHOUSE_DATABASE}_third"

echo '--- the legacy samples alias is a user table once the modern helper exists ---'
# The runtime prefers `.inner.samples.<name>` and only falls back to `.inner.data.<name>`, so with
# the modern helper present that older name belongs to whoever created it.
LEGACY_ALIAS_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy_alias"
rm -rf "$LEGACY_ALIAS_PATH"
$CLICKHOUSE_LOCAL --path "$LEGACY_ALIAS_PATH" --multiquery --query "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${DB} ENGINE = Ordinary;
SET allow_experimental_time_series_table = 1;
CREATE TABLE ${DB}.aaa_ts ENGINE = TimeSeries;
CREATE TABLE ${DB}.\`.inner.data.aaa_ts\` (x UInt8) ENGINE = Memory;
"
LEGACY_ALIAS_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy_alias_dump.sql"
$CLICKHOUSE_LOCAL --path "$LEGACY_ALIAS_PATH" --dump-schema="${DB}" > "$LEGACY_ALIAS_DUMP_FILE" 2>"$ERR_FILE"
echo "legacy-named user table present: $(grep -c "CREATE TABLE ${DB}\.\`\.inner\.data\.aaa_ts\` " "$LEGACY_ALIAS_DUMP_FILE")"
echo "engine-owned helpers in dump: $(grep -c "CREATE TABLE ${DB}\.\`\.inner\.\(samples\|tags\|metrics\)\.aaa_ts\`" "$LEGACY_ALIAS_DUMP_FILE")"
rm -rf "$LEGACY_ALIAS_PATH" "$LEGACY_ALIAS_DUMP_FILE"

echo '--- a dump containing gated object types replays into a default session ---'
# TimeSeries and Ordinary need settings that are off by default, so the dump has to carry them or
# the replay stops at the first such CREATE.
GATED_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_gated"
rm -rf "$GATED_PATH"
$CLICKHOUSE_LOCAL --path "$GATED_PATH" --multiquery --query "
SET allow_experimental_time_series_table = 1;
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${DB} ENGINE = Ordinary;
CREATE TABLE ${DB}.aaa_ts ENGINE = TimeSeries;
"
GATED_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_gated_dump.sql"
$CLICKHOUSE_LOCAL --path "$GATED_PATH" --dump-schema="${DB}" > "$GATED_DUMP_FILE" 2>"$ERR_FILE"
echo "dump enables the Ordinary setting: $(grep -c '^SET allow_deprecated_database_ordinary = 1;' "$GATED_DUMP_FILE")"
echo "dump enables the TimeSeries setting: $(grep -c '^SET allow_experimental_time_series_table = 1;' "$GATED_DUMP_FILE")"
GATED_REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_gated_replay"
rm -rf "$GATED_REPLAY_PATH"
$CLICKHOUSE_LOCAL --path "$GATED_REPLAY_PATH" --queries-file "$GATED_DUMP_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: replayed into a default session' || echo 'FAIL: replay needed settings the dump did not carry'
echo "replayed TimeSeries table present: $($CLICKHOUSE_LOCAL --path "$GATED_REPLAY_PATH" --query "SELECT count() FROM system.tables WHERE database = '${DB}' AND name = 'aaa_ts'")"
rm -rf "$GATED_PATH" "$GATED_REPLAY_PATH" "$GATED_DUMP_FILE"

echo '--- a dump whose view needs an analyzer-side relaxation replays too ---'
# CREATE re-analyzes this Dynamic GROUP BY, whose enabling session setting is not stored.
ANALYZER_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_analyzer_gated"
rm -rf "$ANALYZER_PATH"
$CLICKHOUSE_LOCAL --path "$ANALYZER_PATH" --multiquery --query "
SET allow_suspicious_types_in_group_by = 1;
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.dyn_src (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW ${DB}.dyn_view AS SELECT d FROM ${DB}.dyn_src GROUP BY d;
"
ANALYZER_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_analyzer_dump.sql"
$CLICKHOUSE_LOCAL --path "$ANALYZER_PATH" --dump-schema="${DB}" > "$ANALYZER_DUMP_FILE" 2>"$ERR_FILE"
ANALYZER_REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_analyzer_replay"
rm -rf "$ANALYZER_REPLAY_PATH"
$CLICKHOUSE_LOCAL --path "$ANALYZER_REPLAY_PATH" --queries-file "$ANALYZER_DUMP_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: replayed into a default session' || echo 'FAIL: replay needed settings the dump did not carry'
echo "replayed Dynamic GROUP BY view present: $($CLICKHOUSE_LOCAL --path "$ANALYZER_REPLAY_PATH" --query "SELECT count() FROM system.tables WHERE database = '${DB}' AND name = 'dyn_view'")"
rm -rf "$ANALYZER_PATH" "$ANALYZER_REPLAY_PATH" "$ANALYZER_DUMP_FILE"


rm -rf "$SRC_PATH" "$DST_PATH" "$DUMP_FILE" "$ERR_FILE" "$DUMP_DIR"
mkdir -p "$SRC_PATH" "$DST_PATH"

# `zzz_source` sorts *after* every `aaa_*` dependent below, so a correct dump only replays if
# ordering comes from real dependency tracking, not from (database, name) happening to sort right.
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --multiquery --query "
CREATE DATABASE ${DB};
USE ${DB};
CREATE TABLE ${DB}.zzz_source (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv (id UInt64, val String) ENGINE = MergeTree ORDER BY id AS SELECT id, val FROM ${DB}.zzz_source;
CREATE DICTIONARY ${DB}.aaa_dict (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'zzz_source' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(0);
CREATE VIEW ${DB}.aaa_plain_view AS SELECT * FROM ${DB}.zzz_source;
CREATE VIEW ${DB}.aaa_chain_view AS SELECT * FROM ${DB}.aaa_plain_view;
CREATE TABLE ${DB}.zzz_target (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv_to TO ${DB}.zzz_target AS SELECT id, val FROM ${DB}.zzz_source;
CREATE TABLE ${DB}.join_mv_base (id UInt64, val2 String) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.zzz_join_mv_view AS SELECT id, val2 FROM ${DB}.join_mv_base;
CREATE MATERIALIZED VIEW ${DB}.aaa_join_mv (id UInt64, val String, val2 String) ENGINE = MergeTree ORDER BY id AS SELECT s.id AS id, s.val AS val, v.val2 AS val2 FROM ${DB}.zzz_source AS s INNER JOIN ${DB}.zzz_join_mv_view AS v ON s.id = v.id;
CREATE MATERIALIZED VIEW ${DB}.aaa_union_mv (id UInt64, val String) ENGINE = MergeTree ORDER BY id AS SELECT id, val FROM ${DB}.zzz_source UNION ALL SELECT id, val2 AS val FROM ${DB}.zzz_join_mv_view;
CREATE TABLE ${DB}.\`.inner.literal_table\` (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB}.\`.inner.explicit_target\` (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv_to_inner_named TO ${DB}.\`.inner.explicit_target\` AS SELECT id, val FROM ${DB}.zzz_source;
CREATE TABLE ${DB}.metrics_source (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE ${DB}.metrics_daily_raw (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE VIEW ${DB}.metrics AS SELECT * FROM ${DB}.metrics_daily_raw;
CREATE VIEW ${DB}.metrics_daily AS SELECT * FROM ${DB}.metrics_source;
CREATE TABLE ${DB}.orders (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE ${DB}.users (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE VIEW ${DB}.audit_orders AS SELECT a, '${DB}.audit_users' AS src FROM ${DB}.orders;
CREATE VIEW ${DB}.audit_users AS SELECT a, '${DB}.audit_orders' AS src FROM ${DB}.users;
CREATE TABLE ${DB}.dep_source (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE VIEW ${DB}.dep_view AS SELECT * FROM ${DB}.dep_source;
CREATE VIEW ${DB}.dep_chain_view AS SELECT * FROM ${DB}.dep_view;
CREATE TABLE ${DB}.\`.tmp.inner.literal_table\` (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY ${DB}.zzz_dictget_dict (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'zzz_source' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(0);
CREATE MATERIALIZED VIEW ${DB}.aaa_mv_dictget (id UInt64, v String) ENGINE = MergeTree ORDER BY id AS SELECT id, dictGet('${DB}.zzz_dictget_dict', 'val', id) AS v FROM ${DB}.zzz_source;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv_dictfn (id UInt64, val String) ENGINE = MergeTree ORDER BY id AS SELECT s.id AS id, dd.val AS val FROM ${DB}.zzz_source AS s LEFT JOIN dictionary(${DB}.zzz_dictget_dict) AS dd ON s.id = dd.id;
CREATE TABLE ${DB}.zzz_joinget_tbl (id UInt64, val2 String) ENGINE = Join(ANY, LEFT, id);
CREATE MATERIALIZED VIEW ${DB}.aaa_mv_joinget (id UInt64, v2 String) ENGINE = MergeTree ORDER BY id AS SELECT dummy::UInt64 AS id, joinGet('${DB}.zzz_joinget_tbl', 'val2', dummy::UInt64) AS v2 FROM system.one;
CREATE TABLE ${DB}.zzz_inview_tbl (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv_inref (id UInt64) ENGINE = MergeTree ORDER BY id AS SELECT dummy::UInt64 AS id FROM system.one WHERE dummy::UInt64 IN ${DB}.zzz_inview_tbl;
CREATE TABLE ${DB}.zzz_merge_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_merge_mv (id UInt64) ENGINE = MergeTree ORDER BY id AS SELECT dummy::UInt64 AS id FROM system.one LEFT JOIN merge('${DB}', '^zzz_merge_source\$') AS m ON dummy::UInt64 = m.id;
CREATE TABLE ${DB}.zzz_loop_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_loop_mv (id UInt64) ENGINE = MergeTree ORDER BY id AS SELECT dummy::UInt64 AS id FROM system.one LEFT JOIN loop(${DB}.zzz_loop_source) AS l ON dummy::UInt64 = l.id;
CREATE MATERIALIZED VIEW ${DB}.collision_target (id UInt64) ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${DB}.zzz_source;
CREATE TABLE ${DB}.\`.tmp.inner.collision_target\` (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE DATABASE ${DB2};
CREATE TABLE ${DB2}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE DATABASE ${DB3};
CREATE DICTIONARY ${DB3}.dict (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't' DB '${DB2}')) LAYOUT(FLAT()) LIFETIME(0);
"

echo '--- dependency ordering ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB}" > "$DUMP_FILE" 2>"$ERR_FILE"

source_line=$(grep -n "CREATE TABLE ${DB}\.zzz_source " "$DUMP_FILE" | cut -d: -f1)
target_line=$(grep -n "CREATE TABLE ${DB}\.zzz_target " "$DUMP_FILE" | cut -d: -f1)
mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv " "$DUMP_FILE" | cut -d: -f1)
dict_line=$(grep -n "CREATE DICTIONARY ${DB}\.aaa_dict " "$DUMP_FILE" | cut -d: -f1)
plain_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_plain_view " "$DUMP_FILE" | cut -d: -f1)
chain_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_chain_view " "$DUMP_FILE" | cut -d: -f1)
mv_to_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv_to " "$DUMP_FILE" | cut -d: -f1)

if [[ "$source_line" -lt "$mv_line" && "$source_line" -lt "$dict_line" && "$source_line" -lt "$plain_view_line" \
    && "$plain_view_line" -lt "$chain_view_line" && "$source_line" -lt "$mv_to_line" && "$target_line" -lt "$mv_to_line" ]]; then
    echo 'OK: every dependent is dumped after what it depends on'
else
    echo 'FAIL: unexpected ordering'
fi

# `zzz_join_mv_view` is a second, untracked source for these MVs (only `zzz_source` is tracked by the
# server); it must still be dumped before them even though it sorts after them alphabetically.
join_view_line=$(grep -n "CREATE VIEW ${DB}\.zzz_join_mv_view " "$DUMP_FILE" | cut -d: -f1)
join_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_join_mv " "$DUMP_FILE" | cut -d: -f1)
union_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_union_mv " "$DUMP_FILE" | cut -d: -f1)

if [[ "$join_view_line" -lt "$join_mv_line" && "$join_view_line" -lt "$union_mv_line" \
    && "$source_line" -lt "$join_mv_line" && "$source_line" -lt "$union_mv_line" ]]; then
    echo 'OK: multi-source MV (JOIN and UNION ALL) dumped after every source, including an untracked one that sorts after it by name'
else
    echo 'FAIL: unexpected multi-source MV ordering'
fi

# `metrics`/`metrics_daily` and `audit_orders`/`audit_users` are a reviewer-reported false-positive
# dependency cycle (substring/string-literal matches); both must dump without an INFINITE_LOOP error.
echo "metrics view present: $(grep -c "CREATE VIEW ${DB}\.metrics " "$DUMP_FILE")"
echo "metrics_daily view present: $(grep -c "CREATE VIEW ${DB}\.metrics_daily " "$DUMP_FILE")"
echo "audit_orders view present: $(grep -c "CREATE VIEW ${DB}\.audit_orders " "$DUMP_FILE")"
echo "audit_users view present: $(grep -c "CREATE VIEW ${DB}\.audit_users " "$DUMP_FILE")"
echo "tmp.inner-named literal user table present: $(grep -c "CREATE TABLE ${DB}\.\`\.tmp\.inner\.literal_table\` " "$DUMP_FILE")"

dep_source_line=$(grep -n "CREATE TABLE ${DB}\.dep_source " "$DUMP_FILE" | cut -d: -f1)
dep_view_line=$(grep -n "CREATE VIEW ${DB}\.dep_view " "$DUMP_FILE" | cut -d: -f1)
dep_chain_view_line=$(grep -n "CREATE VIEW ${DB}\.dep_chain_view " "$DUMP_FILE" | cut -d: -f1)
if [[ "$dep_source_line" -lt "$dep_view_line" && "$dep_view_line" -lt "$dep_chain_view_line" ]]; then
    echo 'OK: genuine view-on-view/view-on-table dependency chain still dumped in dependency order'
else
    echo 'FAIL: unexpected dep chain ordering'
fi

# A reference inside a function argument (`dictGet`/`dictionary()`/`joinGet`/`IN`) must still order
# the dump correctly, not just references in `FROM`/`JOIN` position.
dictget_dict_line=$(grep -n "CREATE DICTIONARY ${DB}\.zzz_dictget_dict " "$DUMP_FILE" | cut -d: -f1)
dictget_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv_dictget " "$DUMP_FILE" | cut -d: -f1)
dictfn_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv_dictfn " "$DUMP_FILE" | cut -d: -f1)
joinget_tbl_line=$(grep -n "CREATE TABLE ${DB}\.zzz_joinget_tbl " "$DUMP_FILE" | cut -d: -f1)
joinget_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv_joinget " "$DUMP_FILE" | cut -d: -f1)
inview_tbl_line=$(grep -n "CREATE TABLE ${DB}\.zzz_inview_tbl " "$DUMP_FILE" | cut -d: -f1)
inref_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv_inref " "$DUMP_FILE" | cut -d: -f1)

if [[ "$dictget_dict_line" -lt "$dictget_mv_line" && "$dictget_dict_line" -lt "$dictfn_mv_line" \
    && "$joinget_tbl_line" -lt "$joinget_mv_line" && "$inview_tbl_line" -lt "$inref_mv_line" ]]; then
    echo 'OK: dictGet/dictionary()/joinGet/IN function-argument references dumped after what they depend on'
else
    echo 'FAIL: unexpected function-argument dependency ordering'
fi

# A real table named like a generated inner table, but referenced by an explicit `TO`, must still be
# dumped (and before its materialized view); a genuine generated inner table must still be filtered out.
inner_named_target_line=$(grep -n "CREATE TABLE ${DB}\.\`\.inner\.explicit_target\` " "$DUMP_FILE" | cut -d: -f1)
inner_named_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv_to_inner_named " "$DUMP_FILE" | cut -d: -f1)
if [[ -n "$inner_named_target_line" && "$inner_named_target_line" -lt "$inner_named_mv_line" ]]; then
    echo 'OK: explicit TO target named like a generated inner table is dumped before its materialized view'
else
    echo 'FAIL: explicit TO target named like a generated inner table is missing or misordered'
fi

# Each MV's only dependency is its merge()/loop() source (FROM is system.one), so this only passes
# if collectMergeAndLoopReferences actually contributes the edge, not by coincidence of another one.
merge_source_line=$(grep -n "CREATE TABLE ${DB}\.zzz_merge_source " "$DUMP_FILE" | cut -d: -f1)
merge_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_merge_mv " "$DUMP_FILE" | cut -d: -f1)
loop_source_line=$(grep -n "CREATE TABLE ${DB}\.zzz_loop_source " "$DUMP_FILE" | cut -d: -f1)
loop_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_loop_mv " "$DUMP_FILE" | cut -d: -f1)
if [[ "$merge_source_line" -lt "$merge_mv_line" && "$loop_source_line" -lt "$loop_mv_line" ]]; then
    echo 'OK: merge()/loop() source dumped before the materialized view that reads it'
else
    echo 'FAIL: unexpected merge()/loop() dependency ordering'
fi

# A real table whose name collides with the `.tmp.inner.<mv name>` leftover-refresh naming scheme
# must still be dumped: the name alone doesn't prove it belongs to that view.
echo "colliding real table present: $(grep -c "CREATE TABLE ${DB}\.\`\.tmp\.inner\.collision_target\` " "$DUMP_FILE")"

echo "generated inner tables in dump: $(grep -c '\.inner_id\.' "$DUMP_FILE")"
echo "literal .inner.-named user table present: $(grep -c "CREATE TABLE ${DB}\.\`\.inner\.literal_table\` " "$DUMP_FILE")"
echo "dictionary statements: $(grep -c 'CREATE DICTIONARY' "$DUMP_FILE")"
echo "materialized view statements: $(grep -c 'CREATE MATERIALIZED VIEW' "$DUMP_FILE")"
echo "plain view statements: $(grep -c 'CREATE VIEW' "$DUMP_FILE")"

echo '--- round-trip ---'
# Replayed exactly as emitted: nothing is injected or rewritten.
echo "context switch emitted: $(grep -c "^USE ${DB};$" "$DUMP_FILE")"
$CLICKHOUSE_LOCAL --path "$DST_PATH" --queries-file "$DUMP_FILE"
# Reuse one local instance; each start reloads the whole replayed catalog.
# Query function-reference MV targets directly because inserting into system.one is impossible.
$CLICKHOUSE_LOCAL --path "$DST_PATH" --multiquery --query "
INSERT INTO ${DB}.join_mv_base VALUES (1, 'world');
INSERT INTO ${DB}.zzz_source VALUES (1, 'hello');
SELECT * FROM ${DB}.aaa_mv;
SELECT dictGet('${DB}.aaa_dict', 'val', 1::UInt64);
SELECT * FROM ${DB}.aaa_plain_view;
SELECT * FROM ${DB}.aaa_chain_view;
SELECT * FROM ${DB}.aaa_mv_to;
SELECT * FROM ${DB}.\`.inner.explicit_target\`;
SELECT * FROM ${DB}.aaa_join_mv;
SELECT * FROM ${DB}.aaa_union_mv ORDER BY val;
INSERT INTO ${DB}.dep_source VALUES (1);
SELECT * FROM ${DB}.dep_chain_view;
SELECT dictGet('${DB}.zzz_dictget_dict', 'val', 1::UInt64);
SELECT val FROM dictionary(${DB}.zzz_dictget_dict) WHERE id = 1;
INSERT INTO ${DB}.zzz_joinget_tbl VALUES (1, 'world');
SELECT joinGet('${DB}.zzz_joinget_tbl', 'val2', 1::UInt64);
INSERT INTO ${DB}.zzz_inview_tbl VALUES (1);
SELECT 1::UInt64 IN ${DB}.zzz_inview_tbl;
"

echo '--- unknown database ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB}_does_not_exist" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'UNKNOWN_DATABASE' "$ERR_FILE"

echo '--- predefined database is rejected ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema=system > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"

echo '--- a selector that names nothing is rejected, not broadened ---'
# ' , ' is non-empty but parses to zero names; falling through to "dump everything" would broaden
# the dump set on malformed input, so both flags fail closed instead.
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema=' , ' > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema --dump-schema-exclude=' , ' > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"

echo '--- an exclude naming an unknown database is rejected, not ignored ---'
# A typo in the exclude would otherwise silently include the database the user meant to skip.
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema --dump-schema-exclude="${DB}_does_not_exist" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'UNKNOWN_DATABASE' "$ERR_FILE"

echo '--- a database-less reference that two dumped databases can satisfy is refused ---'
# The CREATE-time session picked one of them, and that choice is not stored with the object, so
# replaying under USE <own db> could silently rebind it. CREATE keeps a dictionary() name as written.
AMB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_amb"
rm -rf "$AMB_PATH"
$CLICKHOUSE_LOCAL --path "$AMB_PATH" --multiquery --query "
CREATE DATABASE amb_a;
CREATE DATABASE amb_b;
CREATE TABLE amb_a.dsrc (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY amb_a.dd (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB 'amb_a')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY amb_b.dd (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB 'amb_a')) LAYOUT(FLAT()) LIFETIME(0);
USE amb_b;
CREATE VIEW amb_a.uses_dict AS SELECT * FROM dictionary('dd');
"
if $CLICKHOUSE_LOCAL --path "$AMB_PATH" --dump-schema='amb_a,amb_b' > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an ambiguous database-less reference'
else
    echo "ambiguous reference refused: $(grep -c 'more than one dumped database' "$ERR_FILE")"
fi
rm -rf "$AMB_PATH"

echo '--- a database-less reference only a wrong-engine namesake collides with is dumped ---'
# joinGet() binds only a Join table and dictionary() only a dictionary, so a same-named MergeTree
# table or view elsewhere cannot be what the CREATE-time session bound and must not fail the dump.
KIND_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kind"
KIND_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kind.sql"
rm -rf "$KIND_PATH"
$CLICKHOUSE_LOCAL --path "$KIND_PATH" --multiquery --query "
CREATE DATABASE kind_a;
CREATE DATABASE kind_b;
CREATE TABLE kind_a.jt (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE kind_b.jt (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE kind_a.dsrc (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY kind_a.dd (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB 'kind_a')) LAYOUT(FLAT()) LIFETIME(0);
CREATE VIEW kind_b.dd AS SELECT 1::UInt64 AS id;
USE kind_a;
CREATE VIEW kind_a.uses_join AS SELECT joinGet('jt', 'v', toUInt64(1)) AS x;
CREATE VIEW kind_a.uses_dict AS SELECT * FROM dictionary('dd');
"
check_kind_dump()
{
    jt_line=$(grep -n 'CREATE TABLE kind_a\.jt ' "$KIND_DUMP_FILE" | cut -d: -f1)
    join_view_line=$(grep -n 'CREATE VIEW kind_a\.uses_join ' "$KIND_DUMP_FILE" | cut -d: -f1)
    dict_line=$(grep -n 'CREATE DICTIONARY kind_a\.dd ' "$KIND_DUMP_FILE" | cut -d: -f1)
    dict_view_line=$(grep -n 'CREATE VIEW kind_a\.uses_dict ' "$KIND_DUMP_FILE" | cut -d: -f1)
    if [[ -n "$jt_line" && -n "$join_view_line" && -n "$dict_line" && -n "$dict_view_line" \
        && "$jt_line" -lt "$join_view_line" && "$dict_line" -lt "$dict_view_line" ]]; then
        echo "OK: $1"
    else
        echo "FAIL: $1 (jt=$jt_line join_view=$join_view_line dict=$dict_line dict_view=$dict_view_line)"
    fi
}
if $CLICKHOUSE_LOCAL --path "$KIND_PATH" --dump-schema='kind_a,kind_b' > "$KIND_DUMP_FILE" 2>"$ERR_FILE"; then
    check_kind_dump 'wrong-engine namesake in another dumped database is not a competing binding'
else
    echo 'FAIL: dump refused a database-less reference over a wrong-engine namesake'
fi
# The same collision, but now the namesakes live in an omitted database.
if $CLICKHOUSE_LOCAL --path "$KIND_PATH" --dump-schema='kind_a' > "$KIND_DUMP_FILE" 2>"$ERR_FILE"; then
    check_kind_dump 'wrong-engine namesake in an omitted database is not a competing binding'
else
    echo 'FAIL: dump refused a database-less reference over a wrong-engine namesake in an omitted database'
fi
rm -rf "$KIND_PATH" "$KIND_DUMP_FILE"

echo '--- a database-less merge() satisfiable by two dumped databases is refused ---'
MRG_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_mrg"
rm -rf "$MRG_PATH"
$CLICKHOUSE_LOCAL --path "$MRG_PATH" --multiquery --query "
CREATE DATABASE mrg_a;
CREATE DATABASE mrg_b;
CREATE TABLE mrg_a.zzz_src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mrg_b.zzz_src (k UInt64) ENGINE = MergeTree ORDER BY k;
USE mrg_b;
CREATE VIEW mrg_a.reads_merge AS SELECT * FROM merge('', '^zzz_src\$');
"
if $CLICKHOUSE_LOCAL --path "$MRG_PATH" --dump-schema='mrg_a,mrg_b' > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an ambiguous database-less merge()'
else
    echo "ambiguous merge refused: $(grep -c 'more than one database' "$ERR_FILE")"
fi
rm -rf "$MRG_PATH"

echo '--- a database-less merge() bound outside the dump set is refused ---'
SUB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_sub"
rm -rf "$SUB_PATH"
$CLICKHOUSE_LOCAL --path "$SUB_PATH" --multiquery --query "
CREATE DATABASE sub_a;
CREATE DATABASE sub_b;
CREATE TABLE sub_b.zzz_only_b (k UInt64) ENGINE = MergeTree ORDER BY k;
USE sub_b;
CREATE VIEW sub_a.reads_outside AS SELECT * FROM merge('', '^zzz_only_b\$');
"
if $CLICKHOUSE_LOCAL --path "$SUB_PATH" --dump-schema='sub_a' > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded though the merge() binding lies outside the dump set'
else
    echo "outside binding refused: $(grep -c 'outside this dump set' "$ERR_FILE")"
fi
rm -rf "$SUB_PATH"

echo '--- a database-less reference satisfiable by an omitted database is refused, not rebound ---'
# dictionary() stores its argument verbatim, so the create-time session database is unknown:
# both the owning and an omitted database have `dd`, and USE <owning> on replay could rebind it.
UNDUMPED_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_undumped"
rm -rf "$UNDUMPED_PATH"
$CLICKHOUSE_LOCAL --path "$UNDUMPED_PATH" --multiquery --query "
CREATE DATABASE undump_a;
CREATE DATABASE undump_b;
CREATE TABLE undump_a.dsrc (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY undump_a.dd (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB 'undump_a')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY undump_b.dd (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB 'undump_a')) LAYOUT(FLAT()) LIFETIME(0);
USE undump_b;
CREATE VIEW undump_a.uses_dict AS SELECT * FROM dictionary('dd');
"
if $CLICKHOUSE_LOCAL --path "$UNDUMPED_PATH" --dump-schema='undump_a' > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an ambiguous database-less reference to an omitted database'
else
    echo "omitted-database reference refused: $(grep -c 'omitted database' "$ERR_FILE")"
fi
rm -rf "$UNDUMPED_PATH"

echo '--- a plain unqualified reference is stored qualified, so it is reported, not rebound ---'
# CREATE stamps the session database onto plain table identifiers in a view body, so this one is
# stored as undump_b.shared_src: nothing to refuse, and the binding is kept and reported, not rebound.
UNQUAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unqual"
UNQUAL_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unqual.sql"
rm -rf "$UNQUAL_PATH"
$CLICKHOUSE_LOCAL --path "$UNQUAL_PATH" --multiquery --query "
CREATE DATABASE undump_a;
CREATE DATABASE undump_b;
CREATE TABLE undump_a.shared_src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE undump_b.shared_src (k UInt64) ENGINE = MergeTree ORDER BY k;
USE undump_b;
CREATE VIEW undump_a.reads_unqual AS SELECT * FROM shared_src;
"
if $CLICKHOUSE_LOCAL --path "$UNQUAL_PATH" --dump-schema='undump_a' > "$UNQUAL_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "reference kept bound to the omitted database: $(grep -c 'AS SELECT \* FROM undump_b\.shared_src' "$UNQUAL_DUMP_FILE")"
    echo "omitted binding reported: $(grep -c 'undump_a\.reads_unqual depends on undump_b\.shared_src,' "$ERR_FILE")"
else
    echo 'FAIL: dump refused a reference the server had already qualified'
fi
rm -rf "$UNQUAL_PATH" "$UNQUAL_DUMP_FILE"

echo '--- a database-less merge() satisfiable by an omitted database is refused, not rebound ---'
UNDUMPED_MRG_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_undumped_mrg"
rm -rf "$UNDUMPED_MRG_PATH"
$CLICKHOUSE_LOCAL --path "$UNDUMPED_MRG_PATH" --multiquery --query "
CREATE DATABASE undump_mrg_a;
CREATE DATABASE undump_mrg_b;
CREATE TABLE undump_mrg_a.shared_mrg (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE undump_mrg_b.shared_mrg (k UInt64) ENGINE = MergeTree ORDER BY k;
USE undump_mrg_b;
CREATE VIEW undump_mrg_a.reads_merge AS SELECT * FROM merge('', '^shared_mrg\$');
"
if $CLICKHOUSE_LOCAL --path "$UNDUMPED_MRG_PATH" --dump-schema='undump_mrg_a' > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an ambiguous merge() to an omitted database'
else
    echo "omitted-database merge refused: $(grep -c 'omitted' "$ERR_FILE")"
fi
rm -rf "$UNDUMPED_MRG_PATH"

echo '--- a database-less reference whose only namesake is predefined is dumped ---'
# `tables` exists in system and INFORMATION_SCHEMA on every server, and no dump contains a
# predefined database, so a namesake there is not an omitted-database conflict.
PREDEF_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_predef"
PREDEF_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_predef.sql"
rm -rf "$PREDEF_PATH"
$CLICKHOUSE_LOCAL --path "$PREDEF_PATH" --multiquery --query "
CREATE DATABASE predef_a;
CREATE TABLE predef_a.tables (k UInt64) ENGINE = MergeTree ORDER BY k;
USE predef_a;
CREATE VIEW predef_a.reads_loop AS SELECT * FROM loop(tables);
CREATE VIEW predef_a.reads_merge AS SELECT * FROM merge('', '^tables\$');
"
if $CLICKHOUSE_LOCAL --path "$PREDEF_PATH" --dump-schema='predef_a' > "$PREDEF_DUMP_FILE" 2>"$ERR_FILE"; then
    predef_src_line=$(grep -n 'CREATE TABLE predef_a\.tables ' "$PREDEF_DUMP_FILE" | cut -d: -f1)
    predef_loop_line=$(grep -n 'CREATE VIEW predef_a\.reads_loop ' "$PREDEF_DUMP_FILE" | cut -d: -f1)
    predef_merge_line=$(grep -n 'CREATE VIEW predef_a\.reads_merge ' "$PREDEF_DUMP_FILE" | cut -d: -f1)
    if [[ -n "$predef_src_line" && -n "$predef_loop_line" && -n "$predef_merge_line" \
        && "$predef_src_line" -lt "$predef_loop_line" && "$predef_src_line" -lt "$predef_merge_line" ]]; then
        echo 'OK: a predefined namesake is not a competing binding'
    else
        echo "FAIL: predefined-namesake dump incomplete or misordered (src=$predef_src_line loop=$predef_loop_line merge=$predef_merge_line)"
    fi
else
    echo "FAIL: dump refused over a predefined-database namesake: $(cat "$ERR_FILE")"
fi
rm -rf "$PREDEF_PATH" "$PREDEF_DUMP_FILE"

echo '--- the prelude omits dump-specific gates this schema cannot need ---'
# Dump-specific gates are emitted only when the dumped AST contains their markers.
echo "ungated gate emitted: $(grep -c 'SET allow_experimental_time_series_table' "${DUMP_FILE}")"
echo "explicit-uuid gate emitted: $(grep -c 'SET database_replicated_allow_explicit_uuid' "${DUMP_FILE}")"
echo "replicated-args gate emitted: $(grep -c 'SET database_replicated_allow_replicated_engine_arguments' "${DUMP_FILE}")"
# These three cannot gate a replay on any schema (obsolete / readerless / parser-implementation
# switch), so they are dropped even from a dump full of views.
echo "dead gates emitted: $(grep -cE 'SET (allow_experimental_window_functions|allow_experimental_hash_functions|allow_simdjson) = ' "${DUMP_FILE}")"

echo '--- all databases ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema > "${DUMP_FILE}.all" 2>"$ERR_FILE"
echo "target database present: $(grep -c "CREATE DATABASE ${DB}" "${DUMP_FILE}.all")"
echo "system database present: $(grep -c 'CREATE DATABASE system' "${DUMP_FILE}.all")"
echo "information_schema database present: $(grep -c 'CREATE DATABASE information_schema' "${DUMP_FILE}.all")"
# `default` exists on every server, so its CREATE must tolerate the existing one on replay.
echo "default database tolerant: $(grep -c 'CREATE DATABASE IF NOT EXISTS default' "${DUMP_FILE}.all")"

echo '--- combined with --query is rejected (clickhouse-local) ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB}" --query "SELECT 1" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"

echo '--- combined with --query is rejected (clickhouse-client) ---'
$CLICKHOUSE_CLIENT --dump-schema="${DB}" --query "SELECT 1" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"

echo '--- clickhouse-client happy path ---'
# The server-backed frontend, exercised end to end: ClientBase::tryRunDumpSchema and the
# IServerConnection packet loop, not just the argument-validation failure above.
CLIENT_DB="${CLICKHOUSE_DATABASE}_client_dump"
$CLICKHOUSE_CLIENT -mq "
DROP DATABASE IF EXISTS ${CLIENT_DB};
CREATE DATABASE ${CLIENT_DB};
CREATE TABLE ${CLIENT_DB}.zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${CLIENT_DB}.aaa_mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${CLIENT_DB}.zzz_src;
"
CLIENT_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_client_dump.sql"
$CLICKHOUSE_CLIENT --dump-schema="${CLIENT_DB}" > "$CLIENT_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: zero exit code' || echo 'FAIL: expected zero exit code'
echo "database present: $(grep -c "CREATE DATABASE ${CLIENT_DB}" "$CLIENT_DUMP_FILE")"
echo "source table present: $(grep -c "CREATE TABLE ${CLIENT_DB}\.zzz_src " "$CLIENT_DUMP_FILE")"
echo "materialized view present: $(grep -c "CREATE MATERIALIZED VIEW ${CLIENT_DB}\.aaa_mv " "$CLIENT_DUMP_FILE")"
client_src_line=$(grep -n "CREATE TABLE ${CLIENT_DB}\.zzz_src " "$CLIENT_DUMP_FILE" | cut -d: -f1)
client_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${CLIENT_DB}\.aaa_mv " "$CLIENT_DUMP_FILE" | cut -d: -f1)
if [[ -n "$client_src_line" && -n "$client_mv_line" && "$client_src_line" -lt "$client_mv_line" ]]; then
    echo 'OK: dependency order preserved over the client connection'
else
    echo 'FAIL: unexpected dependency ordering from the client'
fi
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLIENT_DB}"

echo '--- a database name containing a comma needs backquoting ---'
COMMA_DB="${CLICKHOUSE_DATABASE}_a,b"
$CLICKHOUSE_CLIENT -mq "
DROP DATABASE IF EXISTS \`${COMMA_DB}\`;
CREATE DATABASE \`${COMMA_DB}\`;
"
# Unquoted it is two names, neither of which exists, so the dump fails instead of silently
# dumping the wrong thing.
$CLICKHOUSE_CLIENT --dump-schema="${COMMA_DB}" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: unquoted name rejected' || echo 'FAIL: expected non-zero exit code'
# Backquoted it selects exactly that database.
$CLICKHOUSE_CLIENT --dump-schema="\`${COMMA_DB}\`" > "${CLIENT_DUMP_FILE}.comma" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: backquoted name accepted' || echo 'FAIL: expected zero exit code'
echo "comma database present: $(grep -cF "CREATE DATABASE \`${COMMA_DB}\`" "${CLIENT_DUMP_FILE}.comma")"
echo "databases in the comma dump: $(grep -c "CREATE DATABASE" "${CLIENT_DUMP_FILE}.comma")"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`${COMMA_DB}\`"

echo '--- a malformed backquoted database name in the list is rejected ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema='`unterminated' > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'Unterminated backquoted database name in list' "$ERR_FILE"
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema='`foo`bar' > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'Unexpected text after a backquoted database name in list' "$ERR_FILE"

echo '--- explicit database list ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB},${DB2}" > "${DUMP_FILE}.list" 2>"$ERR_FILE"
echo "first database present: $(grep -c "CREATE DATABASE ${DB}$" "${DUMP_FILE}.list")"
echo "second database present: $(grep -c "CREATE DATABASE ${DB2}" "${DUMP_FILE}.list")"
echo "unlisted database absent: $(grep -c "CREATE DATABASE ${DB3}" "${DUMP_FILE}.list")"

echo '--- exclude list ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema --dump-schema-exclude="${DB2}" > "${DUMP_FILE}.exclude" 2>"$ERR_FILE"
echo "first database present: $(grep -c "CREATE DATABASE ${DB}$" "${DUMP_FILE}.exclude")"
echo "excluded database present: $(grep -c "CREATE DATABASE ${DB2}" "${DUMP_FILE}.exclude")"
echo "other database still present: $(grep -c "CREATE DATABASE ${DB3}" "${DUMP_FILE}.exclude")"

echo '--- explicit list combined with exclude list is rejected ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB}" --dump-schema-exclude="${DB2}" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"

echo '--- a dependency outside the dumped databases is reported, not silently dropped ---'
# The third database dictionary sources from the second one, so dumping the third alone emits an
# object whose source table the dump never creates; that omission has to be reported, not vanish.
SUBSET_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_subset.sql"
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB3}" > "$SUBSET_DUMP_FILE" 2>"$ERR_FILE"
echo "dictionary still dumped: $(grep -c "CREATE DICTIONARY ${DB3}\.dict " "$SUBSET_DUMP_FILE")"
echo "omitted dependency named: $(grep -c "${DB3}\.dict depends on ${DB2}\.t," "$ERR_FILE")"
echo "omitted dependency explained: $(grep -c 'will not be created by this dump' "$ERR_FILE")"
# Reaching the same subset via --dump-schema-exclude must report it too.
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema --dump-schema-exclude="${DB2}" > /dev/null 2>"$ERR_FILE"
echo "same omission via exclude reported: $(grep -c "${DB3}\.dict depends on ${DB2}\.t," "$ERR_FILE")"
# Dumping both databases together leaves nothing outside the set, so nothing is reported.
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB2},${DB3}" > /dev/null 2>"$ERR_FILE"
echo "no warning when the dependency is in the dump: $(grep -c 'will not be created by this dump' "$ERR_FILE")"
rm -f "$SUBSET_DUMP_FILE"

echo '--- dump to directory ---'
# Confirmation lines/filenames embed ${CLICKHOUSE_DATABASE}, so this reports counts/booleans
# instead of raw output, to keep the .reference file stable regardless of the actual name.
CONFIRM_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_confirm.txt"
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB},${DB2}" --dump-schema-dir="$DUMP_DIR" > "$CONFIRM_FILE" 2>"$ERR_FILE"
echo "confirmation lines: $(grep -c "^Dumped database .* schema to ${DUMP_DIR}/.*\.sql$" "$CONFIRM_FILE")"
echo "files created: $(ls "$DUMP_DIR" | wc -l | tr -d ' ')"
[[ -f "$DUMP_DIR/${DB}.sql" ]] && echo 'OK: first database file exists' || echo 'FAIL: first database file missing'
[[ -f "$DUMP_DIR/${DB2}.sql" ]] && echo 'OK: second database file exists' || echo 'FAIL: second database file missing'
echo "first database file has its table: $(grep -c "CREATE TABLE ${DB}\.zzz_source " "$DUMP_DIR/${DB}.sql")"
echo "second database file has its table: $(grep -c "CREATE TABLE ${DB2}\.t " "$DUMP_DIR/${DB2}.sql")"
# A per-database file has to be self-contained too, so replay one on its own, exactly as written.
DIR_REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dir_replay"
rm -rf "$DIR_REPLAY_PATH"
mkdir -p "$DIR_REPLAY_PATH"
$CLICKHOUSE_LOCAL --path "$DIR_REPLAY_PATH" --queries-file "$DUMP_DIR/${DB}.sql"
echo "per-database file replays its materialized view: $($CLICKHOUSE_LOCAL --path "$DIR_REPLAY_PATH" --query "SELECT count() FROM system.tables WHERE database = '${DB}' AND name = 'aaa_mv'")"
rm -rf "$DIR_REPLAY_PATH"
rm -f "$CONFIRM_FILE"

echo '--- dump-schema-dir without --dump-schema is rejected ---'
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema-dir="$DUMP_DIR" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"

echo '--- cross-database dependency orders the confirmation lines and is noted ---'
# Requests the dependent database (third) before its dependency (second); a correct dump still
# writes/confirms the dependency first, regardless of the order requested on the command line.
CROSSDB_CONFIRM_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_crossdb_confirm.txt"
CROSSDB_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_crossdb_dir"
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB3},${DB2}" --dump-schema-dir="$CROSSDB_DIR" > "$CROSSDB_CONFIRM_FILE" 2>"$ERR_FILE"
dependency_line=$(grep -n "^Dumped database ${DB2} " "$CROSSDB_CONFIRM_FILE" | cut -d: -f1)
dependent_line=$(grep -n "^Dumped database ${DB3} " "$CROSSDB_CONFIRM_FILE" | cut -d: -f1)
if [[ "$dependency_line" -lt "$dependent_line" ]]; then
    echo 'OK: dependency database is dumped/confirmed before the dependent database'
else
    echo 'FAIL: unexpected cross-database ordering'
fi
echo "cross-database note present: $(grep -c 'depend on tables in another dumped database' "$CROSSDB_CONFIRM_FILE")"
rm -f "$CROSSDB_CONFIRM_FILE"
rm -rf "$CROSSDB_DIR"

echo '--- the replay prelude only names settings the source server has ---'
# Filter compiled-in replay settings to names supported by the source server.
PRELUDE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_prelude.sql"
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --dump-schema="${DB}" > "$PRELUDE_DUMP_FILE" 2>"$ERR_FILE"
PRELUDE_NAMES=$(grep -oE '^SET [a-z_0-9]+ = [0-9]+;$' "$PRELUDE_DUMP_FILE" | awk '{print "\047"$2"\047"}' | sort -u | paste -sd, -)
echo "prelude emitted settings: $([[ -n "$PRELUDE_NAMES" ]] && echo yes || echo no)"
echo "unknown to the server: $($CLICKHOUSE_LOCAL --path "$SRC_PATH" --query \
    "SELECT countIf(name NOT IN (SELECT name FROM system.settings)) FROM values('name String', $PRELUDE_NAMES)")"
# A plain dump has neither explicit UUID nor retained replicated-engine arguments, so omits both gates.
echo "replicated engine arguments gate at 3: $(grep -c '^SET database_replicated_allow_replicated_engine_arguments = 3;$' "$PRELUDE_DUMP_FILE")"
echo "explicit uuid gate at 3: $(grep -c '^SET database_replicated_allow_explicit_uuid = 3;$' "$PRELUDE_DUMP_FILE")"
rm -f "$PRELUDE_DUMP_FILE"

# Explicit UUIDs need gate value 3; value 2 would replace the UUID during replay.
UUID_GATED_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_uuid_gated.sql"
$CLICKHOUSE_LOCAL --path "$SRC_PATH" --show_table_uuid_in_table_create_query_if_not_nil=1 --dump-schema="${DB}" > "$UUID_GATED_DUMP_FILE" 2>"$ERR_FILE"
echo "explicit uuid gate at 3 when dumped with uuids: $(grep -c '^SET database_replicated_allow_explicit_uuid = 3;$' "$UUID_GATED_DUMP_FILE")"
rm -f "$UUID_GATED_DUMP_FILE"

# Retained replicated-engine arguments need quiet gate value 3 and a ZooKeeper-backed fixture.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.zzz_repl_engine_args (x UInt64) ENGINE = ReplicatedMergeTree('/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dump_schema/zzz_repl_engine_args', 'r') ORDER BY x"
REPL_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_repl.sql"
$CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$REPL_DUMP_FILE" 2>"$ERR_FILE"
echo "replicated engine arguments gate at 3 when the dump keeps the arguments: $(grep -c '^SET database_replicated_allow_replicated_engine_arguments = 3;$' "$REPL_DUMP_FILE")"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.zzz_repl_engine_args"
rm -f "$REPL_DUMP_FILE"

rm -rf "$SRC_PATH" "$DST_PATH" "$DUMP_FILE" "${DUMP_FILE}.all" "${DUMP_FILE}.list" "${DUMP_FILE}.exclude" "$DUMP_DIR" "$ERR_FILE"

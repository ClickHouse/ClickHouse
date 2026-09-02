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
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.aaa_ts ENGINE = TimeSeries;
"
GATED_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_gated_dump.sql"
$CLICKHOUSE_LOCAL --path "$GATED_PATH" --dump-schema="${DB}" > "$GATED_DUMP_FILE" 2>"$ERR_FILE"
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
# replaying under USE <own db> could silently rebind it.
AMB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_amb"
rm -rf "$AMB_PATH"
$CLICKHOUSE_LOCAL --path "$AMB_PATH" --multiquery --query "
CREATE DATABASE amb_a;
CREATE DATABASE amb_b;
CREATE TABLE amb_a.jt (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE amb_b.jt (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
USE amb_b;
CREATE VIEW amb_a.uses_join AS SELECT joinGet('jt', 'v', toUInt64(1)) AS x;
"
if $CLICKHOUSE_LOCAL --path "$AMB_PATH" --dump-schema='amb_a,amb_b' > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an ambiguous database-less reference'
else
    echo "ambiguous reference refused: $(grep -c 'more than one dumped database' "$ERR_FILE")"
fi
rm -rf "$AMB_PATH"

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
    echo "ambiguous merge refused: $(grep -c 'more than one dumped database' "$ERR_FILE")"
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
# Both the owning database and an omitted database have a table with the same name; the view was
# created under USE <omitted>, so replaying under USE <owning> would silently rebind the reference.
UNDUMPED_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_undumped"
rm -rf "$UNDUMPED_PATH"
$CLICKHOUSE_LOCAL --path "$UNDUMPED_PATH" --multiquery --query "
CREATE DATABASE undump_a;
CREATE DATABASE undump_b;
CREATE TABLE undump_a.shared_src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE undump_b.shared_src (k UInt64) ENGINE = MergeTree ORDER BY k;
USE undump_b;
CREATE VIEW undump_a.reads_unqual AS SELECT * FROM shared_src;
"
if $CLICKHOUSE_LOCAL --path "$UNDUMPED_PATH" --dump-schema='undump_a' > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an ambiguous database-less reference to an omitted database'
else
    echo "omitted-database reference refused: $(grep -c 'omitted database' "$ERR_FILE")"
fi
rm -rf "$UNDUMPED_PATH"

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

echo '--- database names colliding only by case are rejected for --dump-schema-dir ---'
CASE_DB1="${CLICKHOUSE_DATABASE}_MixedCase"
CASE_DB2="${CLICKHOUSE_DATABASE}_mixedcase"
$CLICKHOUSE_CLIENT -mq "
DROP DATABASE IF EXISTS \`${CASE_DB1}\`;
DROP DATABASE IF EXISTS \`${CASE_DB2}\`;
CREATE DATABASE \`${CASE_DB1}\`;
CREATE DATABASE \`${CASE_DB2}\`;
"
CASE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_case_dir"
rm -rf "$CASE_DIR"
$CLICKHOUSE_CLIENT --dump-schema="${CASE_DB1},${CASE_DB2}" --dump-schema-dir="$CASE_DIR" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'would all be written to the same file' "$ERR_FILE"
$CLICKHOUSE_CLIENT -mq "
DROP DATABASE \`${CASE_DB1}\`;
DROP DATABASE \`${CASE_DB2}\`;
"
rm -rf "$CASE_DIR"

echo '--- circular cross-database table dependencies are rejected for --dump-schema-dir ---'
CIRC_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_circ"
rm -rf "$CIRC_PATH"
$CLICKHOUSE_LOCAL --path "$CIRC_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
CREATE TABLE ${DB}.a1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB2}.b1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB2}.v_on_a1 AS SELECT * FROM ${DB}.a1;
CREATE VIEW ${DB}.v_on_b1 AS SELECT * FROM ${DB2}.b1;
"
CIRC_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_circ_dir"
rm -rf "$CIRC_DIR"
$CLICKHOUSE_LOCAL --path "$CIRC_PATH" --dump-schema="${DB},${DB2}" --dump-schema-dir="$CIRC_DIR" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'circular cross-database table dependencies' "$ERR_FILE"
$CLICKHOUSE_LOCAL --path "$CIRC_PATH" --dump-schema="${DB},${DB2}" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: plain dump without --dump-schema-dir still succeeds (no table-level cycle)' || echo 'FAIL: plain dump unexpectedly failed'
rm -rf "$CIRC_PATH" "$CIRC_DIR"

echo '--- combined with --file/--structure is rejected ---'
echo '1,2' > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv"
$CLICKHOUSE_LOCAL --dump-schema --file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv" --structure="a UInt8, b UInt8" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'BAD_ARGUMENTS' "$ERR_FILE"
rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv"

echo '--- merge()/loop() with a computed (non-literal) argument fails clearly instead of silently dropping the dependency ---'
CONSTEXPR_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_constexpr"
rm -rf "$CONSTEXPR_PATH"
$CLICKHOUSE_LOCAL --path "$CONSTEXPR_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.zzz_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv (id UInt64) ENGINE = MergeTree ORDER BY id AS
    SELECT s.id FROM ${DB}.zzz_source AS s LEFT JOIN merge(concat('${DB}', ''), '^zzz_source\$') AS m ON s.id = m.id;
"
$CLICKHOUSE_LOCAL --path "$CONSTEXPR_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'NOT_IMPLEMENTED' "$ERR_FILE"
rm -rf "$CONSTEXPR_PATH"

echo '--- a qualified cross-database dictionary() reference keeps its real dependency ---'
# The same shape as the refusals below, but qualified: the dump keeps the ${DB2} edge and orders
# the dictionary before the view that reads it, which is what qualifying the reference buys.
QUAL_REF_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_qual_ref"
rm -rf "$QUAL_REF_PATH"
$CLICKHOUSE_LOCAL --path "$QUAL_REF_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
CREATE TABLE ${DB2}.zzz_src (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY ${DB2}.zzz_dict (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'zzz_src' DB '${DB2}')) LAYOUT(FLAT()) LIFETIME(0);
CREATE VIEW ${DB}.aaa_view AS SELECT * FROM dictionary('${DB2}.zzz_dict');
"
QUAL_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_qual_dump.sql"
$CLICKHOUSE_LOCAL --path "$QUAL_REF_PATH" --dump-schema="${DB},${DB2}" > "$QUAL_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: dump succeeded' || echo 'FAIL: dump failed on a qualified reference'
qual_dict_line=$(grep -n "CREATE DICTIONARY ${DB2}\.zzz_dict " "$QUAL_DUMP_FILE" | cut -d: -f1)
qual_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_view " "$QUAL_DUMP_FILE" | cut -d: -f1)
if [[ -n "$qual_dict_line" && -n "$qual_view_line" && "$qual_dict_line" -lt "$qual_view_line" ]]; then
    echo 'OK: qualified dictionary() dependency ordered before its reader'
else
    echo 'FAIL: qualified dictionary() dependency missing or misordered'
fi
rm -rf "$QUAL_REF_PATH" "$QUAL_DUMP_FILE"

echo '--- a database-less dictionary() reference in a view is refused, not rebound ---'
# The stored unqualified dictionary name would rebind from ${DB2} at CREATE to ${DB} on replay.
UNQUAL_DICTFN_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unqual_dictfn"
rm -rf "$UNQUAL_DICTFN_PATH"
$CLICKHOUSE_LOCAL --path "$UNQUAL_DICTFN_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
USE ${DB2};
CREATE TABLE zzz_src (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY zzz_dict (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'zzz_src' DB '${DB2}')) LAYOUT(FLAT()) LIFETIME(0);
CREATE VIEW ${DB}.aaa_view AS SELECT * FROM dictionary('zzz_dict');
"
$CLICKHOUSE_LOCAL --path "$UNQUAL_DICTFN_PATH" --dump-schema="${DB},${DB2}" > "$DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'resolves against the session database' "$ERR_FILE"
echo "rebound dump emitted: $(grep -c 'CREATE VIEW' "$DUMP_FILE")"
rm -rf "$UNQUAL_DICTFN_PATH"

echo '--- a database-less joinGet() reference in a materialized view is refused, not rebound ---'
UNQUAL_JOINGET_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unqual_joinget"
rm -rf "$UNQUAL_JOINGET_PATH"
$CLICKHOUSE_LOCAL --path "$UNQUAL_JOINGET_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
USE ${DB2};
CREATE TABLE zzz_join (id UInt64, val String) ENGINE = Join(ANY, LEFT, id);
CREATE MATERIALIZED VIEW ${DB}.aaa_mv (id UInt64, v String) ENGINE = MergeTree ORDER BY id AS SELECT dummy::UInt64 AS id, joinGet('zzz_join', 'val', dummy::UInt64) AS v FROM system.one;
"
$CLICKHOUSE_LOCAL --path "$UNQUAL_JOINGET_PATH" --dump-schema="${DB},${DB2}" > "$DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'resolves against the session database' "$ERR_FILE"
echo "rebound dump emitted: $(grep -c 'CREATE MATERIALIZED VIEW' "$DUMP_FILE")"
rm -rf "$UNQUAL_JOINGET_PATH"

echo '--- an IN <table> reference keeps the database it was created under ---'
# Stored IN table references are qualified; preserve and order the resulting ${DB2} edge.
UNQUAL_IN_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unqual_in"
rm -rf "$UNQUAL_IN_PATH"
$CLICKHOUSE_LOCAL --path "$UNQUAL_IN_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
USE ${DB2};
CREATE TABLE zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_view AS SELECT dummy::UInt64 AS id FROM system.one WHERE dummy::UInt64 IN zzz_src;
"
$CLICKHOUSE_LOCAL --path "$UNQUAL_IN_PATH" --dump-schema="${DB},${DB2}" > "$DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: dump succeeded' || echo 'FAIL: dump failed'
in_src_line=$(grep -n "CREATE TABLE ${DB2}\.zzz_src " "$DUMP_FILE" | cut -d: -f1)
in_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_view " "$DUMP_FILE" | cut -d: -f1)
if [[ -n "$in_src_line" && -n "$in_view_line" && "$in_src_line" -lt "$in_view_line" ]]; then
    echo 'OK: IN table reference keeps its create-time database dependency'
else
    echo 'FAIL: IN table reference dependency missing or misordered'
fi
rm -rf "$UNQUAL_IN_PATH"

echo '--- a database-less one-argument loop(table) reference is refused, not rebound ---'
UNQUAL_LOOP_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unqualified_loop"
rm -rf "$UNQUAL_LOOP_PATH"
$CLICKHOUSE_LOCAL --path "$UNQUAL_LOOP_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
USE ${DB2};
CREATE TABLE zzz_loop_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_loop_mv (id UInt64) ENGINE = MergeTree ORDER BY id AS
    SELECT dummy::UInt64 AS id FROM system.one LEFT JOIN loop(zzz_loop_source) AS l ON dummy::UInt64 = l.id;
"
$CLICKHOUSE_LOCAL --path "$UNQUAL_LOOP_PATH" --dump-schema="${DB},${DB2}" > "$DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'resolves against the session database' "$ERR_FILE"
echo "rebound dump emitted: $(grep -c 'CREATE MATERIALIZED VIEW' "$DUMP_FILE")"
rm -rf "$UNQUAL_LOOP_PATH"

echo '--- a database-less reference is satisfied from the object database inside the dump set ---'
# The replay restores USE before the CREATE, so an unqualified reference is sound whenever the
# object's own database provides it; the edge must still order the dictionary first.
UNQUAL_SAMEDB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unqual_samedb"
rm -rf "$UNQUAL_SAMEDB_PATH"
$CLICKHOUSE_LOCAL --path "$UNQUAL_SAMEDB_PATH" --multiquery --query "
CREATE DATABASE ${DB};
USE ${DB};
CREATE TABLE zzz_src (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY zzz_dict (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'zzz_src' DB '${DB}')) LAYOUT(FLAT()) LIFETIME(0);
CREATE VIEW aaa_view AS SELECT * FROM dictionary('zzz_dict');
"
$CLICKHOUSE_LOCAL --path "$UNQUAL_SAMEDB_PATH" --dump-schema="${DB}" > "$DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: zero exit code' || echo 'FAIL: expected zero exit code'
unqual_dict_line=$(grep -n "CREATE DICTIONARY ${DB}\.zzz_dict" "$DUMP_FILE" | cut -d: -f1)
unqual_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_view " "$DUMP_FILE" | cut -d: -f1)
if [[ -n "$unqual_dict_line" && -n "$unqual_view_line" && "$unqual_dict_line" -lt "$unqual_view_line" ]]; then
    echo 'OK: unqualified dictionary ordered before its reader'
else
    echo 'FAIL: unqualified dependency not ordered'
fi
rm -rf "$UNQUAL_SAMEDB_PATH"

echo '--- a computed joinGet/dictionary argument fails clearly instead of being treated as dependency-free ---'
COMPUTED_DICT_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_computed_dict"
rm -rf "$COMPUTED_DICT_PATH"
$CLICKHOUSE_LOCAL --path "$COMPUTED_DICT_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.zzz_join (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
CREATE VIEW ${DB}.aaa_view AS SELECT joinGet(concat('${DB}', '.zzz_join'), 'v', 1::UInt64) AS v;
"
$CLICKHOUSE_LOCAL --path "$COMPUTED_DICT_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -ne 0 ]] && echo 'OK: non-zero exit code' || echo 'FAIL: expected non-zero exit code'
grep -o -m1 'NOT_IMPLEMENTED' "$ERR_FILE"
rm -rf "$COMPUTED_DICT_PATH"

echo '--- merge() naming a database outside the dump set warns instead of silently dropping the edge ---'
EXTERNAL_MERGE_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_external_merge"
rm -rf "$EXTERNAL_MERGE_PATH"
$CLICKHOUSE_LOCAL --path "$EXTERNAL_MERGE_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE DATABASE ${DB2};
CREATE TABLE ${DB2}.zzz_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB}.yyy_local (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv (id UInt64) ENGINE = MergeTree ORDER BY id AS
    SELECT s.id FROM ${DB}.yyy_local AS s LEFT JOIN merge('${DB2}', '^zzz_source\$') AS m ON s.id = m.id;
"
$CLICKHOUSE_LOCAL --path "$EXTERNAL_MERGE_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: dump succeeded' || echo 'FAIL: dump failed'
grep -o -m1 'outside the dumped database(s)' "$ERR_FILE"
rm -rf "$EXTERNAL_MERGE_PATH"

echo '--- a large as_select (over the old query-size default) still gets its dependencies tracked ---'
LARGE_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_large"
rm -rf "$LARGE_PATH"
LARGE_PAD=$(printf '%*s' 270000 '' | tr ' ' 'x')
# A 270000-byte literal embedded in --query would blow past the OS argv-size limit on some CI
# runners; a --queries-file has no such limit. Still needs a higher client-side max_query_size.
LARGE_SETUP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_large_setup.sql"
cat > "$LARGE_SETUP_FILE" <<EOF
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.zzz_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_large_view AS SELECT id, '${LARGE_PAD}' AS pad FROM ${DB}.zzz_source;
EOF
$CLICKHOUSE_LOCAL --path "$LARGE_PATH" --max_query_size=1000000 --queries-file "$LARGE_SETUP_FILE"
rm -f "$LARGE_SETUP_FILE"
LARGE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_large_dump.sql"
$CLICKHOUSE_LOCAL --path "$LARGE_PATH" --dump-schema="${DB}" > "$LARGE_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: large as_select dumped successfully' || echo 'FAIL: dump failed on a large but valid as_select'
large_source_line=$(grep -n "CREATE TABLE ${DB}\.zzz_source " "$LARGE_DUMP_FILE" | cut -d: -f1)
large_view_line=$(grep -n "CREATE VIEW ${DB}\.aaa_large_view " "$LARGE_DUMP_FILE" | cut -d: -f1)
if [[ -n "$large_source_line" && -n "$large_view_line" && "$large_source_line" -lt "$large_view_line" ]]; then
    echo 'OK: dependency correctly tracked for a large as_select'
else
    echo 'FAIL: dependency not tracked for a large as_select'
fi
rm -rf "$LARGE_PATH" "$LARGE_DUMP_FILE"

echo '--- a large CREATE with an explicit TO target named like inner storage is still classified correctly ---'
# Same size pressure as the as_select case above, but on `create_table_query`: a size-capped reparse
# fails, and taking that to mean "no explicit TO" would drop this real target table from the dump.
LARGE_TO_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_large_to"
rm -rf "$LARGE_TO_PATH"
LARGE_TO_SETUP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_large_to_setup.sql"
cat > "$LARGE_TO_SETUP_FILE" <<EOF
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.zzz_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB}.\`.inner.large_target\` (id UInt64, pad String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB}.aaa_mv_large_to TO ${DB}.\`.inner.large_target\` AS SELECT id, '${LARGE_PAD}' AS pad FROM ${DB}.zzz_source;
EOF
$CLICKHOUSE_LOCAL --path "$LARGE_TO_PATH" --max_query_size=1000000 --queries-file "$LARGE_TO_SETUP_FILE"
rm -f "$LARGE_TO_SETUP_FILE"
LARGE_TO_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_large_to_dump.sql"
$CLICKHOUSE_LOCAL --path "$LARGE_TO_PATH" --dump-schema="${DB}" > "$LARGE_TO_DUMP_FILE" 2>"$ERR_FILE"
large_to_target_line=$(grep -n "CREATE TABLE ${DB}\.\`\.inner\.large_target\` " "$LARGE_TO_DUMP_FILE" | cut -d: -f1)
large_to_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv_large_to " "$LARGE_TO_DUMP_FILE" | cut -d: -f1)
if [[ -n "$large_to_target_line" && -n "$large_to_mv_line" && "$large_to_target_line" -lt "$large_to_mv_line" ]]; then
    echo 'OK: large explicit-TO target named like generated inner storage is dumped before its materialized view'
else
    echo 'FAIL: large explicit-TO target named like generated inner storage is missing or misordered'
fi
rm -rf "$LARGE_TO_PATH" "$LARGE_TO_DUMP_FILE"

echo '--- a table function inside remote() is not a local dependency ---'
# A merge under remote runs elsewhere; treating it as local invents a cycle with the real b -> a edge.
# Create its remote source through the ordinary server while keeping both views in the local catalog.
REMOTE_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_remote"
rm -rf "$REMOTE_PATH"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.b (id UInt64) ENGINE = Memory"
$CLICKHOUSE_LOCAL --path "$REMOTE_PATH" --multiquery "
    CREATE DATABASE ${DB};
    CREATE VIEW ${DB}.a AS SELECT * FROM remote('127.0.0.2', merge('${DB}', '^b\$'));
    CREATE VIEW ${DB}.b AS SELECT * FROM ${DB}.a;
"
REMOTE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_remote_dump.sql"
if $CLICKHOUSE_LOCAL --path "$REMOTE_PATH" --dump-schema="${DB}" > "$REMOTE_DUMP_FILE" 2>"$ERR_FILE"; then
    echo 'OK: remote() table function did not invent a local cycle'
else
    echo "FAIL: dump rejected: $(cat "$ERR_FILE")"
fi
echo "both views dumped: $(grep -c "CREATE VIEW ${DB}\.[ab] " "$REMOTE_DUMP_FILE")"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.b"
rm -rf "$REMOTE_PATH" "$REMOTE_DUMP_FILE"

echo '--- a table function under a cluster with no local replicas is not a local dependency ---'
# This cluster resolves on the ordinary server but has no replica local to this instance.
CLUSTER_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_cluster"
CLUSTER_CONF="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_cluster.xml"
rm -rf "$CLUSTER_PATH"
cat > "$CLUSTER_CONF" <<EOF
<clickhouse>
    <tcp_port>9999</tcp_port>
    <remote_servers>
        <dump_schema_remote_only>
            <shard>
                <replica>
                    <host>127.0.0.2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </dump_schema_remote_only>
    </remote_servers>
</clickhouse>
EOF
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.b (id UInt64) ENGINE = Memory"
$CLICKHOUSE_LOCAL --config-file "$CLUSTER_CONF" --path "$CLUSTER_PATH" --multiquery "
    CREATE DATABASE ${DB};
    CREATE VIEW ${DB}.a AS SELECT * FROM cluster('dump_schema_remote_only', merge('${DB}', '^b\$'));
    CREATE VIEW ${DB}.b AS SELECT * FROM ${DB}.a;
"
CLUSTER_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_cluster_dump.sql"
if $CLICKHOUSE_LOCAL --config-file "$CLUSTER_CONF" --path "$CLUSTER_PATH" --dump-schema="${DB}" > "$CLUSTER_DUMP_FILE" 2>"$ERR_FILE"; then
    echo 'OK: remote-only cluster() table function did not invent a local cycle'
else
    echo "FAIL: dump rejected: $(cat "$ERR_FILE")"
fi
echo "both views dumped: $(grep -c "CREATE VIEW ${DB}\.[ab] " "$CLUSTER_DUMP_FILE")"

echo '--- a cluster the dumping instance does not define fails clearly instead of guessing ---'
# The same catalog dumped without the cluster config: the walker cannot classify the call, and
# guessing either way risks an invented cycle or a missed edge, so the dump refuses loudly.
if $CLICKHOUSE_LOCAL --path "$CLUSTER_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an unresolvable cluster'
else
    echo "unresolvable cluster refused: $(grep -c 'Cannot resolve cluster' "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.b"
rm -rf "$CLUSTER_PATH" "$CLUSTER_CONF" "$CLUSTER_DUMP_FILE"

echo '--- a cluster with local replicas names a real local dependency ---'
# Local cluster arguments are dependencies, including bare identifiers and constant expressions.
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_cluster_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_cluster_reader AS SELECT * FROM cluster(test_shard_localhost, '${DB}', 'zzz_cluster_src');
CREATE VIEW ${DB}.aab_cluster_reader_ident AS SELECT * FROM cluster(test_shard_localhost, '${DB}', zzz_cluster_src);
CREATE VIEW ${DB}.aac_cluster_reader_cexpr AS SELECT * FROM cluster(concat('test_', 'shard_localhost'), '${DB}', 'zzz_cluster_src');
CREATE VIEW ${DB}.aad_cluster_reader_dbexpr AS SELECT * FROM cluster(test_shard_localhost, concat('${DB}', ''), concat('zzz_cluster_src', ''));
CREATE VIEW ${DB}.aae_cluster_reader_qexpr AS SELECT * FROM cluster(test_shard_localhost, concat('${DB}.zzz_cluster_src', ''));
CREATE VIEW ${DB}.aaf_cluster_reader_merge AS SELECT * FROM cluster(test_shard_localhost, merge('${DB}', '^zzz_cluster_src\$'));
"
LOCAL_CLUSTER_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local_cluster_dump.sql"
$CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$LOCAL_CLUSTER_DUMP_FILE" 2>"$ERR_FILE"
SRC_LINE=$(grep -n "CREATE TABLE ${DB}\.zzz_cluster_src" "$LOCAL_CLUSTER_DUMP_FILE" | head -1 | cut -d: -f1)
READER_LINE=$(grep -n "CREATE VIEW ${DB}\.aaa_cluster_reader" "$LOCAL_CLUSTER_DUMP_FILE" | head -1 | cut -d: -f1)
IDENT_READER_LINE=$(grep -n "CREATE VIEW ${DB}\.aab_cluster_reader_ident" "$LOCAL_CLUSTER_DUMP_FILE" | head -1 | cut -d: -f1)
if [ -n "$SRC_LINE" ] && [ -n "$READER_LINE" ] && [ "$SRC_LINE" -lt "$READER_LINE" ]; then
    echo 'OK: local cluster() source dumped before its reader'
else
    echo "FAIL: local cluster() dependency missing or misordered (src=$SRC_LINE reader=$READER_LINE)"
fi
if [ -n "$SRC_LINE" ] && [ -n "$IDENT_READER_LINE" ] && [ "$SRC_LINE" -lt "$IDENT_READER_LINE" ]; then
    echo 'OK: identifier-spelled local cluster() source dumped before its reader'
else
    echo "FAIL: identifier-spelled local cluster() dependency missing or misordered (src=$SRC_LINE reader=$IDENT_READER_LINE)"
fi
CEXPR_LINE=$(grep -n "CREATE VIEW ${DB}\.aac_cluster_reader_cexpr" "$LOCAL_CLUSTER_DUMP_FILE" | head -1 | cut -d: -f1)
DBEXPR_LINE=$(grep -n "CREATE VIEW ${DB}\.aad_cluster_reader_dbexpr" "$LOCAL_CLUSTER_DUMP_FILE" | head -1 | cut -d: -f1)
QEXPR_LINE=$(grep -n "CREATE VIEW ${DB}\.aae_cluster_reader_qexpr" "$LOCAL_CLUSTER_DUMP_FILE" | head -1 | cut -d: -f1)
if [ -n "$SRC_LINE" ] && [ -n "$CEXPR_LINE" ] && [ "$SRC_LINE" -lt "$CEXPR_LINE" ]; then
    echo 'OK: computed cluster name classified as local and ordered'
else
    echo "FAIL: computed cluster name (src=$SRC_LINE reader=$CEXPR_LINE)"
fi
if [ -n "$SRC_LINE" ] && [ -n "$DBEXPR_LINE" ] && [ "$SRC_LINE" -lt "$DBEXPR_LINE" ]; then
    echo 'OK: computed db/table arguments name the local source'
else
    echo "FAIL: computed db/table arguments (src=$SRC_LINE reader=$DBEXPR_LINE)"
fi
if [ -n "$SRC_LINE" ] && [ -n "$QEXPR_LINE" ] && [ "$SRC_LINE" -lt "$QEXPR_LINE" ]; then
    echo 'OK: computed qualified argument names the local source'
else
    echo "FAIL: computed qualified argument (src=$SRC_LINE reader=$QEXPR_LINE)"
fi
# A local cluster() may wrap another table function; the guard must not read `merge` as an
# unresolvable scalar and refuse - the recursive walk orders the inner reference instead.
MERGE_LINE=$(grep -n "CREATE VIEW ${DB}\.aaf_cluster_reader_merge" "$LOCAL_CLUSTER_DUMP_FILE" | head -1 | cut -d: -f1)
if [ -n "$SRC_LINE" ] && [ -n "$MERGE_LINE" ] && [ "$SRC_LINE" -lt "$MERGE_LINE" ]; then
    echo 'OK: merge() wrapped in a local cluster() names the local source'
else
    echo "FAIL: merge() wrapped in local cluster() (src=$SRC_LINE reader=$MERGE_LINE)"
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_cluster_reader;
DROP TABLE ${DB}.aab_cluster_reader_ident;
DROP TABLE ${DB}.aac_cluster_reader_cexpr;
DROP TABLE ${DB}.aad_cluster_reader_dbexpr;
DROP TABLE ${DB}.aae_cluster_reader_qexpr;
DROP TABLE ${DB}.aaf_cluster_reader_merge;
DROP TABLE ${DB}.zzz_cluster_src;
"
rm -f "$LOCAL_CLUSTER_DUMP_FILE"

# The server folds session context inside wrapped table functions into stored literals at CREATE.
# The dump must accept that stable binding and order the source first.
WRAPPED_MERGE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_wrapped_merge.sql"
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_cluster_merge_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_cluster_merge_sessiondb AS SELECT * FROM cluster(test_shard_localhost, merge(currentDatabase(), '^zzz_cluster_merge_src\$'));
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$WRAPPED_MERGE_DUMP_FILE" 2>"$ERR_FILE"; then
    if grep -qF "merge('${DB}'," "$WRAPPED_MERGE_DUMP_FILE"; then
        echo 'OK: session context inside the wrapped merge() is folded into the stored view'
    else
        echo 'FAIL: wrapped merge() session context not folded into the stored view'
    fi
    WRAPPED_READER_LINE=$(grep -n "CREATE VIEW ${DB}.aaa_cluster_merge_sessiondb" "$WRAPPED_MERGE_DUMP_FILE" | head -1 | cut -d: -f1)
    WRAPPED_SRC_LINE=$(grep -n "CREATE TABLE ${DB}.zzz_cluster_merge_src" "$WRAPPED_MERGE_DUMP_FILE" | head -1 | cut -d: -f1)
    if [ -n "$WRAPPED_READER_LINE" ] && [ -n "$WRAPPED_SRC_LINE" ] && [ "$WRAPPED_SRC_LINE" -lt "$WRAPPED_READER_LINE" ]; then
        echo 'OK: folded wrapped merge() source dumped before its reader'
    else
        echo 'FAIL: folded wrapped merge() source not ordered before its reader'
    fi
else
    echo 'FAIL: dump refused despite the folded wrapped merge()'
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_cluster_merge_sessiondb;
DROP TABLE ${DB}.zzz_cluster_merge_src;
"
rm -f "$WRAPPED_MERGE_DUMP_FILE"

# Empty merge databases resolve to the owner under the replay's USE statement.
EMPTYDB_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_emptydb.sql"
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_merge_emptydb_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_merge_emptydb AS SELECT * FROM merge('', '^zzz_merge_emptydb_src\$');
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$EMPTYDB_DUMP_FILE" 2>"$ERR_FILE"; then
    EMPTYDB_READER_LINE=$(grep -n "CREATE VIEW ${DB}.aaa_merge_emptydb" "$EMPTYDB_DUMP_FILE" | head -1 | cut -d: -f1)
    EMPTYDB_SRC_LINE=$(grep -n "CREATE TABLE ${DB}.zzz_merge_emptydb_src" "$EMPTYDB_DUMP_FILE" | head -1 | cut -d: -f1)
    if [ -n "$EMPTYDB_READER_LINE" ] && [ -n "$EMPTYDB_SRC_LINE" ] && [ "$EMPTYDB_SRC_LINE" -lt "$EMPTYDB_READER_LINE" ]; then
        echo 'OK: empty-database merge() resolves against the owning database'
    else
        echo 'FAIL: empty-database merge() did not order its source before its reader'
    fi
else
    echo 'FAIL: dump refused for the empty-database merge()'
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_merge_emptydb;
DROP TABLE ${DB}.zzz_merge_emptydb_src;
"
rm -f "$EMPTYDB_DUMP_FILE"

echo '--- a cluster() argument reading the session database is refused, not rebound ---'
# The server folded `currentDatabase()` against the session that ran the CREATE; the dump session's
# own database is a different one, so folding it here would silently rebind or drop the edge.
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_cluster_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_cluster_reader_sessiondb AS SELECT * FROM cluster(test_shard_localhost, currentDatabase(), 'zzz_cluster_src');
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite a session-database argument'
else
    echo "session-database cluster() argument refused: $(grep -c 'read the session database' "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_cluster_reader_sessiondb;
DROP TABLE ${DB}.zzz_cluster_src;
"

echo '--- a cluster() argument reading the session user is refused too ---'
# currentUser is not stored; a database-per-user cluster reference must fail closed.
CURUSER=$($CLICKHOUSE_CLIENT -q "SELECT currentUser()")
CURUSER_TBL="zzz_curuser_${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE \`${CURUSER}\`.\`${CURUSER_TBL}\` (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_cluster_reader_curuser AS SELECT * FROM cluster(test_shard_localhost, currentUser(), '${CURUSER_TBL}');
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite a session-user argument'
else
    echo "session-user cluster() argument refused: $(grep -c 'session database or user' "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -mq "
DROP VIEW ${DB}.aaa_cluster_reader_curuser;
DROP TABLE \`${CURUSER}\`.\`${CURUSER_TBL}\`;
"

echo '--- a cluster() argument reading the database setting is refused too ---'
# `USE` mirrors the chosen database into the `database` setting, so getSetting('database') folds to
# the session database exactly as currentDatabase() does and has to fail closed the same way.
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_cluster_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_cluster_reader_getsetting AS SELECT * FROM cluster(test_shard_localhost, getSetting('database'), 'zzz_cluster_src');
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite a database-setting argument'
else
    echo "database-setting cluster() argument refused: $(grep -c 'read the session database' "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_cluster_reader_getsetting;
DROP TABLE ${DB}.zzz_cluster_src;
"

echo '--- a cluster() argument reading any other setting is refused too ---'
# No setting's create-time value is stored with the object, so `database` is not a special case:
# the server bound this to the value `log_comment` held at CREATE time, which the dump cannot know.
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_cluster_src_other (id UInt64) ENGINE = MergeTree ORDER BY id;
SET log_comment = '${DB}';
CREATE VIEW ${DB}.aaa_cluster_reader_othersetting AS SELECT * FROM cluster(test_shard_localhost, getSetting('log_comment'), 'zzz_cluster_src_other');
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an argument reading a setting'
else
    echo "other-setting cluster() argument refused: $(grep -c 'a session setting' "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_cluster_reader_othersetting;
DROP TABLE ${DB}.zzz_cluster_src_other;
"

echo '--- a cluster() argument reading a server constant is refused too ---'
# hostName is server identity; folding it on a remote client would change the table binding.
HOSTNAME_TBL=$($CLICKHOUSE_CLIENT -q "SELECT hostName()")
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.\`${HOSTNAME_TBL}\` (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_cluster_reader_hostname AS SELECT * FROM cluster(test_shard_localhost, '${DB}', hostName());
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump succeeded despite an argument reading a server constant'
else
    echo "server-constant cluster() argument refused: $(grep -c 'a server constant' "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -mq "
DROP VIEW ${DB}.aaa_cluster_reader_hostname;
DROP TABLE ${DB}.\`${HOSTNAME_TBL}\`;
"

echo '--- a cluster() name written as a macro resolves ---'
# Stored cluster macros must be expanded before lookup; this one resolves to test_shard_localhost.
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_cluster_src_macro (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_cluster_reader_macro AS SELECT * FROM cluster('{default_cluster_macro}', '${DB}', 'zzz_cluster_src_macro');
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'macro cluster name resolved: 1'
else
    echo "macro cluster name resolved: 0 ($(grep -c 'Cannot resolve cluster' "$ERR_FILE"))"
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_cluster_reader_macro;
DROP TABLE ${DB}.zzz_cluster_src_macro;
"

echo '--- a cluster() name written as a nested macro resolves ---'
# Macro substitutions are recursively expanded to the same depth as the server.
NESTED_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nested"
NESTED_CONF="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nested.xml"
rm -rf "$NESTED_PATH"
cat > "$NESTED_CONF" <<EOF
<clickhouse>
    <tcp_port>9999</tcp_port>
    <macros>
        <dump_schema_macro_inner>dump_schema_nested_cluster</dump_schema_macro_inner>
        <dump_schema_macro_outer>{dump_schema_macro_inner}</dump_schema_macro_outer>
    </macros>
    <remote_servers>
        <dump_schema_nested_cluster>
            <shard>
                <replica>
                    <host>127.0.0.2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </dump_schema_nested_cluster>
    </remote_servers>
</clickhouse>
EOF
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.b (id UInt64) ENGINE = Memory"
$CLICKHOUSE_LOCAL --config-file "$NESTED_CONF" --path "$NESTED_PATH" --multiquery "
    CREATE DATABASE ${DB};
    CREATE VIEW ${DB}.a AS SELECT * FROM cluster('{dump_schema_macro_outer}', '${DB}', 'b');
"
if $CLICKHOUSE_LOCAL --config-file "$NESTED_CONF" --path "$NESTED_PATH" --dump-schema="${DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'nested macro cluster name resolved: 1'
else
    echo "nested macro cluster name resolved: 0 ($(grep -c 'Cannot resolve cluster' "$ERR_FILE"))"
fi
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.b"
rm -rf "$NESTED_PATH" "$NESTED_CONF"

echo '--- a TimeSeries table is dumped after its explicitly named external target ---'
# TimeSeries target edges come only from its stored CREATE and must override lexical order.
TS_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_ts"
rm -rf "$TS_PATH"
$CLICKHOUSE_LOCAL --path "$TS_PATH" --multiquery "
    CREATE DATABASE ${DB};
    SET allow_experimental_time_series_table = 1;
    CREATE TABLE ${DB}.zzz_ts_metrics (metric_family_name String, type String, unit String, help String)
        ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
    CREATE TABLE ${DB}.aaa_ts ENGINE = TimeSeries METRICS ${DB}.zzz_ts_metrics;
"
TS_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_ts_dump.sql"
$CLICKHOUSE_LOCAL --path "$TS_PATH" --dump-schema="${DB}" > "$TS_DUMP_FILE" 2>"$ERR_FILE"
echo "engine-owned inner tables in TimeSeries dump: $(grep -c '\.inner' "$TS_DUMP_FILE")"
TS_TARGET_LINE=$(grep -n "CREATE TABLE ${DB}\.zzz_ts_metrics" "$TS_DUMP_FILE" | head -1 | cut -d: -f1)
TS_LINE=$(grep -n "CREATE TABLE ${DB}\.aaa_ts " "$TS_DUMP_FILE" | head -1 | cut -d: -f1)
if [ -n "$TS_TARGET_LINE" ] && [ -n "$TS_LINE" ] && [ "$TS_TARGET_LINE" -lt "$TS_LINE" ]; then
    echo 'OK: TimeSeries external target dumped before the TimeSeries table'
else
    echo "FAIL: TimeSeries target ordering (target=$TS_TARGET_LINE ts=$TS_LINE)"
fi
rm -rf "$TS_PATH" "$TS_DUMP_FILE"

echo '--- a database-less TimeSeries target keeps the session database it was created under ---'
# A TimeSeries target created under USE ${DB2} must retain that qualified external edge.
TS_UNQUAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_ts_unqual"
rm -rf "$TS_UNQUAL_PATH"
$CLICKHOUSE_LOCAL --path "$TS_UNQUAL_PATH" --multiquery "
    CREATE DATABASE ${DB};
    CREATE DATABASE ${DB2};
    USE ${DB2};
    SET allow_experimental_time_series_table = 1;
    CREATE TABLE zzz_ts_metrics (metric_family_name String, type String, unit String, help String)
        ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
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
CREATE TABLE ${DB}.real_metrics (metric_family_name String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
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
CREATE TABLE ${DB2}.jj (id UInt64, v UInt64) ENGINE = Join(ANY, LEFT, id);
CREATE TABLE ${DB}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
USE ${DB2};
CREATE MATERIALIZED VIEW ${DB}.mv ENGINE = MergeTree ORDER BY id AS SELECT id, joinGet('jj', 'v', id) AS v FROM ${DB}.src;
"
DBLESS_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dbless_dump.sql"
$CLICKHOUSE_LOCAL --path "$DBLESS_PATH" --dump-schema="${DB}" > "$DBLESS_DUMP_FILE" 2>"$ERR_FILE"
echo "dump still emits the view: $(grep -c "CREATE MATERIALIZED VIEW ${DB}\.mv " "$DBLESS_DUMP_FILE")"
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
# Shared experimental settings are emitted unconditionally because CREATE replay re-enters
# many of them (view/projection analysis, default-expression validation, suspicious-type checks).
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

echo '--- the prelude carries all shared experimental settings unconditionally ---'
# Shared experimental settings are emitted for every dump because CREATE replay re-enters
# many of them. Verify a plain table dump still carries representative gates from the shared list.
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

echo '--- a dump with a UniqueMergeTree table replays through the prelude ---'
# UniqueMergeTree requires allow_experimental_unique_key, which the prelude carries unconditionally.
UNIQUE_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unique"
UNIQUE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unique.sql"
rm -rf "$UNIQUE_PATH"
$CLICKHOUSE_LOCAL --path "$UNIQUE_PATH" --multiquery --query "
SET allow_experimental_unique_key = 1;
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.uniq (id UInt64) ENGINE = UniqueMergeTree ORDER BY id;
"
$CLICKHOUSE_LOCAL --path "$UNIQUE_PATH" --dump-schema="${DB}" > "$UNIQUE_DUMP_FILE" 2>"$ERR_FILE"
UNIQUE_REPLAY_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unique_replay"
rm -rf "$UNIQUE_REPLAY_PATH"
$CLICKHOUSE_LOCAL --path "$UNIQUE_REPLAY_PATH" --queries-file "$UNIQUE_DUMP_FILE" 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: replayed into a default session' || echo 'FAIL: replay needed settings the dump did not carry'
echo "replayed UniqueMergeTree table present: $($CLICKHOUSE_LOCAL --path "$UNIQUE_REPLAY_PATH" --query "SELECT count() FROM system.tables WHERE database = '${DB}' AND name = 'uniq' AND engine = 'UniqueMergeTree'")"
rm -rf "$UNIQUE_PATH" "$UNIQUE_REPLAY_PATH" "$UNIQUE_DUMP_FILE"

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

#!/usr/bin/env bash
# Tags: long, no-darwin
# Second of three 04836_client_dump_schema files; uses many local instances.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dump.sql"
ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_err.txt"

DB="${CLICKHOUSE_DATABASE}"
DB2="${CLICKHOUSE_DATABASE}_second"

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
grep -o -m1 'would be written to the same file' "$ERR_FILE"
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

echo '--- a joinGet() reference in a materialized view keeps the database it was created under ---'
# CREATE qualifies the joinGet() table name with the session database; keep and order that ${DB2} edge.
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
[[ $rc -eq 0 ]] && echo 'OK: dump succeeded' || echo 'FAIL: dump failed'
jg_join_line=$(grep -n "CREATE TABLE ${DB2}\.zzz_join " "$DUMP_FILE" | cut -d: -f1)
jg_mv_line=$(grep -n "CREATE MATERIALIZED VIEW ${DB}\.aaa_mv " "$DUMP_FILE" | cut -d: -f1)
if [[ -n "$jg_join_line" && -n "$jg_mv_line" && "$jg_join_line" -lt "$jg_mv_line" ]]; then
    echo 'OK: joinGet() reference keeps its create-time database dependency'
else
    echo 'FAIL: joinGet() reference dependency missing or misordered'
fi
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

# Empty merge databases resolve to the owner under the replay's USE statement. The source name has
# to be unique server-wide: an empty database is ambiguous exactly when another database holds a
# matching table, and a concurrent run of this file in its own database is such a namesake.
EMPTYDB_SRC="zzz_merge_emptydb_src_${CLICKHOUSE_TEST_UNIQUE_NAME}"
EMPTYDB_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_emptydb.sql"
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.${EMPTYDB_SRC} (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_merge_emptydb AS SELECT * FROM merge('', '^${EMPTYDB_SRC}\$');
"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$EMPTYDB_DUMP_FILE" 2>"$ERR_FILE"; then
    EMPTYDB_READER_LINE=$(grep -n "CREATE VIEW ${DB}.aaa_merge_emptydb" "$EMPTYDB_DUMP_FILE" | head -1 | cut -d: -f1)
    EMPTYDB_SRC_LINE=$(grep -n "CREATE TABLE ${DB}.${EMPTYDB_SRC}" "$EMPTYDB_DUMP_FILE" | head -1 | cut -d: -f1)
    if [ -n "$EMPTYDB_READER_LINE" ] && [ -n "$EMPTYDB_SRC_LINE" ] && [ "$EMPTYDB_SRC_LINE" -lt "$EMPTYDB_READER_LINE" ]; then
        echo 'OK: empty-database merge() resolves against the owning database'
    else
        echo 'FAIL: empty-database merge() did not order its source before its reader'
    fi
else
    echo "FAIL: dump refused for the empty-database merge(): $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_merge_emptydb;
DROP TABLE ${DB}.${EMPTYDB_SRC};
"
rm -f "$EMPTYDB_DUMP_FILE"

echo '--- a remote() address the server reads locally names a real local dependency ---'
# The server marks loopback replicas on its own port local (`Cluster::Address::isLocal`) and reads
# their tables from its catalog, so a reader over such a `remote()` must come after its source,
# whether the address spells the port or not, and whether it names the table or wraps `merge()`.
# Every reader's only in-dump edge is the `remote()` one (the MV reads `system.one`), so without it
# all sit at dependency level 0 and sort by name before `zzz_remote_src`.
$CLICKHOUSE_CLIENT -mq "
CREATE TABLE ${DB}.zzz_remote_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.aaa_remote_reader AS SELECT * FROM remote('127.0.0.1', '${DB}', 'zzz_remote_src');
CREATE VIEW ${DB}.aab_remote_reader_port AS SELECT * FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', ${DB}.zzz_remote_src);
CREATE VIEW ${DB}.aac_remote_reader_merge AS SELECT * FROM remote('localhost', merge('${DB}', '^zzz_remote_src\$'));
CREATE MATERIALIZED VIEW ${DB}.aad_remote_mv (id UInt64) ENGINE = Memory AS
    SELECT dummy::UInt64 AS id FROM system.one WHERE dummy::UInt64 IN (SELECT id FROM remote('127.0.0.1', ${DB}.zzz_remote_src));
CREATE VIEW ${DB}.aae_remote_reader_case AS SELECT * FROM remote('LOCALHOST', '${DB}', 'zzz_remote_src');
CREATE VIEW ${DB}.aaf_remote_reader_case_port AS SELECT * FROM remote('LocalHost:${CLICKHOUSE_PORT_TCP}', ${DB}.zzz_remote_src);
CREATE VIEW ${DB}.aag_remote_secure_reader AS SELECT * FROM remoteSecure('127.0.0.1:${CLICKHOUSE_PORT_TCP_SECURE}', '${DB}', 'zzz_remote_src');
"
LOCAL_REMOTE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local_remote_dump.sql"
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$LOCAL_REMOTE_DUMP_FILE" 2>"$ERR_FILE"; then
    SRC_LINE=$(grep -n "CREATE TABLE ${DB}\.zzz_remote_src" "$LOCAL_REMOTE_DUMP_FILE" | head -1 | cut -d: -f1)
    # The mixed-case readers are local too because hostnames are case-insensitive, so their source must come first.
    for reader in aaa_remote_reader aab_remote_reader_port aac_remote_reader_merge aad_remote_mv \
                  aae_remote_reader_case aaf_remote_reader_case_port aag_remote_secure_reader; do
        READER_LINE=$(grep -n "CREATE \(MATERIALIZED \)\?VIEW ${DB}\.${reader} " "$LOCAL_REMOTE_DUMP_FILE" | head -1 | cut -d: -f1)
        if [ -n "$SRC_LINE" ] && [ -n "$READER_LINE" ] && [ "$SRC_LINE" -lt "$READER_LINE" ]; then
            echo "OK: local remote() source dumped before ${reader}"
        else
            echo "FAIL: local remote() dependency of ${reader} missing or misordered (src=$SRC_LINE reader=$READER_LINE)"
        fi
    done
else
    echo "FAIL: dump rejected: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT -mq "
DROP TABLE ${DB}.aaa_remote_reader;
DROP TABLE ${DB}.aab_remote_reader_port;
DROP TABLE ${DB}.aac_remote_reader_merge;
DROP TABLE ${DB}.aad_remote_mv;
DROP TABLE ${DB}.aae_remote_reader_case;
DROP TABLE ${DB}.aaf_remote_reader_case_port;
DROP TABLE ${DB}.aag_remote_secure_reader;
DROP TABLE ${DB}.zzz_remote_src;
"
rm -f "$LOCAL_REMOTE_DUMP_FILE"

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
    CREATE TABLE ${DB}.zzz_ts_metrics (metric_family String, type String, unit String, help String)
        ENGINE = ReplacingMergeTree ORDER BY metric_family;
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

rm -f "$DUMP_FILE" "$ERR_FILE"

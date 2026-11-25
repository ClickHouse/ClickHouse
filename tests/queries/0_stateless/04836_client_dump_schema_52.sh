#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the XGBoost contrib is not built in the fast test.

# --dump-schema of a data path refuses a table whose TTL, projection or secondary index would load a model dictionary through
# `predictXGBoost` or `naiveBayesClassifier*` on constants, or through `assignCentroid` in a subquery, before the source command runs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Loads a source with model dictionaries whose sources run a command, and the given table, and tells whether the dump ran one.
# A MergeTree TTL reads `enable_xgboost` from the server's own settings, so every instance gets it.
dump_with_commands() {
    local name=$1
    rm -rf "$work/src" "$work/ran"
    $CLICKHOUSE_LOCAL --path "$work/src" --enable_xgboost=1 -n -q "
    CREATE DATABASE d;
    CREATE DICTIONARY d.model (x Float64, y Float64) PRIMARY KEY x
        SOURCE(EXECUTABLE(COMMAND 'echo ran >> $work/ran; printf \"1,2\\\\n2,4\\\\n3,6\\\\n\"' FORMAT 'CSV'))
        LAYOUT(XGBOOST(num_iterations 3)) LIFETIME(0);
    CREATE DICTIONARY d.nb (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0) PRIMARY KEY ngram
        SOURCE(EXECUTABLE(COMMAND 'echo ran >> $work/ran; printf \"good,0,5\\\\nbad,1,5\\\\n\"' FORMAT 'CSV'))
        LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' alpha 1.0)) LIFETIME(0);
    CREATE DICTIONARY d.centroids (cid UInt64, vec Array(Float32)) PRIMARY KEY cid
        SOURCE(EXECUTABLE(COMMAND 'echo ran >> $work/ran; printf \"1\\\\t[0,0]\\\\n2\\\\t[5,5]\\\\n\"' FORMAT 'TSV'))
        LAYOUT(FLAT()) LIFETIME(0);
    $2;"
    rm -f "$work/ran"
    $CLICKHOUSE_LOCAL --path "$work/src" --enable_xgboost=1 --dump-schema d > "$work/dump.sql" 2> "$work/dump.err"
    echo "$name: dump failed: $([ $? -ne 0 ] && echo 1 || echo 0)"
    grep -o 'would run when the table is loaded' "$work/dump.err" | head -1
    echo "$name: table dumped: $(grep -c '^CREATE TABLE d.t ' "$work/dump.sql")"
    if [ -e "$work/ran" ]; then echo "$name: FAIL: a command ran"; else echo "$name: no command ran"; fi
}

table="CREATE TABLE d.t (ts DateTime, k UInt64"
# CREATE refuses a non-deterministic function in a secondary index, so such a table is one attached with its full definition.
attach="ATTACH TABLE d.t UUID '6a5d4c3b-2a19-4087-9f6e-5d4c3b2a1908' (ts DateTime, k UInt64"

echo '--- a model function on constants in a TTL, a projection or a secondary index is refused ---'
dump_with_commands xgboost_ttl "$table) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k < toUInt64(predictXGBoost('d.model', toFloat64(1)))"
dump_with_commands xgboost_projection "$table, PROJECTION p (SELECT k + toUInt64(predictXGBoost('d.model', toFloat64(1))) ORDER BY k))
    ENGINE = MergeTree ORDER BY k"
dump_with_commands xgboost_index "$attach, INDEX i k + toUInt64(predictXGBoost('d.model', toFloat64(1))) TYPE minmax)
    ENGINE = MergeTree ORDER BY k"
dump_with_commands naive_bayes_ttl "$table) ENGINE = MergeTree ORDER BY k TTL ts DELETE WHERE k < naiveBayesClassifier('d.nb', 'good')"
dump_with_commands naive_bayes_identifier "$table) ENGINE = MergeTree ORDER BY k TTL ts DELETE WHERE k < naiveBayesClassifier(d.nb, 'good')"
dump_with_commands naive_bayes_with_prob "$table, PROJECTION p (SELECT k + naiveBayesClassifierWithProb('d.nb', 'good').1 ORDER BY k))
    ENGINE = MergeTree ORDER BY k"
dump_with_commands naive_bayes_with_all_probs "$attach, INDEX i k + length(naiveBayesClassifierWithAllProbs('d.nb', 'good')) TYPE minmax)
    ENGINE = MergeTree ORDER BY k"
dump_with_commands centroid_subquery "$table) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k < (SELECT assignCentroid([1.0, 1.0]::Array(Float32), 'd.centroids'))"

echo '--- a model function that reads a column, or assignCentroid outside a subquery, is not run, and the table is dumped ---'
dump_with_commands xgboost_column "$table) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k < toUInt64(predictXGBoost('d.model', toFloat64(k)))"
dump_with_commands naive_bayes_column "$table) ENGINE = MergeTree ORDER BY k TTL ts DELETE WHERE k < naiveBayesClassifier(d.nb, toString(k))"
dump_with_commands centroid_column "$table, v Array(Float32), PROJECTION p (SELECT k, assignCentroid(v, 'd.centroids') ORDER BY k))
    ENGINE = MergeTree ORDER BY k"
dump_with_commands centroid_constant "$table) ENGINE = MergeTree ORDER BY k
    TTL ts DELETE WHERE k < assignCentroid([1.0, 1.0]::Array(Float32), 'd.centroids')"

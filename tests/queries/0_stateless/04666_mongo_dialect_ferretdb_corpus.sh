#!/usr/bin/env bash
# Runs a corpus of Mongo dialect queries and checks only that the server survives it.
#
# The corpus is `data_mongo/ferretdb_query_corpus.txt`, derived from the integration test suite of
# FerretDB - https://github.com/FerretDB/FerretDB, Copyright 2021 FerretDB Inc., licensed under
# the Apache License, Version 2.0. That suite runs each of its cases against a real MongoDB server
# and asserts both answer alike, so its inputs are the shapes a MongoDB client actually sends,
# including the ones a MongoDB server rejects. The file itself carries the full attribution and
# the list of changes made to the original work.
#
# What is asserted is deliberately narrow, and only what stays true as more of MongoDB is
# implemented: the whole corpus is consumed, every query is answered with either a result or a
# controlled exception, no query produces a logical error - which would be a bug in the server
# rather than a rejected query - and the server is still there afterwards. The results themselves
# are not asserted: FerretDB compares against a MongoDB server, and a collection whose fields are
# columns of one type each cannot always answer the same, so a reference of results here would
# record this implementation's answers rather than MongoDB's.
#
# Four inputs of this corpus used to segfault the server; those are also kept, with their errors,
# in 04664_mongo_dialect_unsupported.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CORPUS="$CUR_DIR"/data_mongo/ferretdb_query_corpus.txt

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS corpus;
    CREATE TABLE corpus (_id String, v Nullable(Int64), s String, arr Array(Int64)) ENGINE = MergeTree ORDER BY _id;
    INSERT INTO corpus VALUES ('a', 42, 'foo', [1, 2, 3]), ('b', NULL, '', []);
"

# The corpus runs as one multi query so that the whole of it costs one connection rather than one
# per query. `--ignore-error` is what lets it continue past the queries that are rejected, which
# is most of them. The temporary directory is shared with the tests running at the same time, so
# the file names carry the database name, which is not.
QUERIES="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.sql"
{
    echo "SET dialect = 'mongo';"
    grep -v '^#' "$CORPUS" | sed 's/$/;/'
    # Switching back out of the dialect and reading the marker proves the whole file was consumed:
    # a query that killed the client or the server would leave it unread.
    echo "SET dialect = 'clickhouse';"
    echo "SELECT 'corpus consumed';"
} > "$QUERIES"

echo "queries in the corpus: $(grep -vc '^#' "$CORPUS")"

${CLICKHOUSE_CLIENT} --multiquery --ignore-error --queries-file "$QUERIES" \
    > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.out" 2> "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.err"

grep -qx 'corpus consumed' "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.out" && echo 'the whole corpus was consumed'

# Every exception must carry a code and a message. A truncated or garbled one would not, and
# neither would a report that is not an exception at all. (A parse failure the client itself
# reports has no code, which is how `clickhouse-client` reports one for every dialect, so only
# the lines that do announce a code are compared.)
if [ "$(grep -c '^Code: ' "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.err")" \
     = "$(grep -cE '^Code: [0-9]+\. DB::Exception: .+' "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.err")" ]; then
    echo 'every exception is well formed'
fi

# A logical error means the server reached a state it holds to be impossible, which a query must
# not be able to do whatever it asks for.
grep -q 'LOGICAL_ERROR\|Logical error' "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.err" || echo 'no logical error'
grep -qi 'Segmentation fault\|Sanitizer\|Assertion.*failed\|Stack trace' "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.err" \
    || echo 'no crash'

${CLICKHOUSE_CLIENT} --query "SELECT 'the server is still there'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE corpus"
rm -f "$QUERIES" "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.out" "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_ferretdb_corpus.err"

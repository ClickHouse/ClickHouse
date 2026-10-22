#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses the S3 mock server on localhost:11111

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The set of a `GLOBAL IN` predicate on `_file` / `_path` is only created while the pipeline runs, so
# `DisclosedGlobIterator` cannot prune the first batch of a glob while the pipeline is built and the
# number of survivors is unknown when the streams are sized. Sizing them from the raw batch would start
# a stream per possible address: the ones whose address the filter then rejects ask the generator past
# `glob_expansion_max_elements` while the survivor is still being read, failing the query. A deferred
# filter must therefore get a single stream, which prunes the batch as soon as it runs.

prefix="${CLICKHOUSE_DATABASE}_glob"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('http://localhost:11111/test/${prefix}_5.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 5"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('http://localhost:11111/test/${prefix}_7.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 7"

echo "--- one stream while the filter is deferred, not one per buffered address"
# `enable_parallel_replicas` would replace the `URL` source with a cluster read, hiding the
# pipeline shape this test pins.
$CLICKHOUSE_CLIENT --query "EXPLAIN PIPELINE SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..19}.tsv', 'TSV', 'x UInt64') WHERE _file GLOBAL IN (SELECT '${prefix}_5.tsv') SETTINGS glob_expansion_max_elements = 10, max_threads = 4, enable_parallel_replicas = 0" \
    | grep -vF "ReadFromURL" | grep -oE "URL( × [0-9]+)?"

echo "--- and the early survivors are read without asking the generator past the limit"
# One thread, so that the `LIMIT` stops the pipeline before the single stream pulls its next address.
$CLICKHOUSE_CLIENT --query "SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..19}.tsv', 'TSV', 'x UInt64') WHERE _file GLOBAL IN (SELECT '${prefix}_5.tsv' UNION ALL SELECT '${prefix}_7.tsv') LIMIT 1 SETTINGS glob_expansion_max_elements = 10, max_threads = 1, enable_parallel_replicas = 0"

echo "--- the deferred filter still selects exactly the matching addresses"
$CLICKHOUSE_CLIENT --query "SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..9}.tsv', 'TSV', 'x UInt64') WHERE _file GLOBAL IN (SELECT '${prefix}_5.tsv' UNION ALL SELECT '${prefix}_7.tsv') ORDER BY x SETTINGS glob_expansion_max_elements = 10, max_threads = 4, enable_parallel_replicas = 0"

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses the S3 mock server on localhost:11111

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `_file` predicate prunes the batch the pattern's first addresses are generated into, and the
# number of streams must follow the survivors, not the size of the pattern: a reader started for an
# address that does not exist asks the generator for one as soon as it starts, pushing it past the
# limit and failing the query even though everything it reads was generated within it.

prefix="${CLICKHOUSE_DATABASE}_glob"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('http://localhost:11111/test/${prefix}_5.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 5"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('http://localhost:11111/test/${prefix}_7.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 7"

echo "--- one stream for the one survivor of the first batch, not one per possible address"
# `enable_parallel_replicas` would replace the `URL` source with a cluster read, hiding the
# pipeline shape this test pins.
$CLICKHOUSE_CLIENT --query "EXPLAIN PIPELINE SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..19}.tsv', 'TSV', 'x UInt64') WHERE _file = '${prefix}_5.tsv' SETTINGS glob_expansion_max_elements = 10, max_threads = 4, enable_parallel_replicas = 0" \
    | grep -vF "ReadFromURL" | grep -oE "URL( × [0-9]+)?"

echo "--- and the surviving addresses are the ones read"
$CLICKHOUSE_CLIENT --query "SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..19}.tsv', 'TSV', 'x UInt64') WHERE _file IN ('${prefix}_5.tsv', '${prefix}_7.tsv') LIMIT 1 SETTINGS glob_expansion_max_elements = 10, max_threads = 1"

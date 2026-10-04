#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: uses the S3 mock server on localhost:11111
# Tag no-parallel: the failpoint is server-wide and fires for the next `url` read with a `_file` filter

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The set of a `GLOBAL IN` predicate on `_file` / `_path` can be left unbuilt while the pipeline is
# built, and then `DisclosedGlobIterator` defers the pruning to the first `next`. A local read builds its
# sets early, so the failpoint `url_glob_defer_path_filter` forces the deferral here. The number of
# survivors is unknown while the streams are sized, so a single stream is only needed when the pattern
# is larger than `glob_expansion_max_elements`: an extra stream could then ask the generator past the
# limit. When the whole pattern fits into the limit, the query must keep its parallelism, even when the
# survivors all live after the first batch of 1000 addresses.

prefix="${CLICKHOUSE_DATABASE}_glob"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('http://localhost:11111/test/${prefix}_1500.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 1500"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('http://localhost:11111/test/${prefix}_1700.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 1700"

filter="_file GLOBAL IN (SELECT '${prefix}_1500.tsv' UNION ALL SELECT '${prefix}_1700.tsv')"

# `enable_parallel_replicas` would replace the `URL` source with a cluster read, hiding the
# pipeline shape this test pins.
echo "--- the pattern fits into the limit: as many streams as requested"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT url_glob_defer_path_filter"
$CLICKHOUSE_CLIENT --query "EXPLAIN PIPELINE SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..1999}.tsv', 'TSV', 'x UInt64') WHERE ${filter} SETTINGS glob_expansion_max_elements = 2000, max_threads = 4, enable_parallel_replicas = 0" \
    | grep -vF "ReadFromURL" | grep -oE "URL( × [0-9]+)?"

echo "--- and they read exactly the survivors"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT url_glob_defer_path_filter"
$CLICKHOUSE_CLIENT --query "SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..1999}.tsv', 'TSV', 'x UInt64') WHERE ${filter} ORDER BY x SETTINGS glob_expansion_max_elements = 2000, max_threads = 4, enable_parallel_replicas = 0"

echo "--- the pattern is larger than the limit: one stream"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT url_glob_defer_path_filter"
$CLICKHOUSE_CLIENT --query "EXPLAIN PIPELINE SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..2000}.tsv', 'TSV', 'x UInt64') WHERE ${filter} SETTINGS glob_expansion_max_elements = 2000, max_threads = 4, enable_parallel_replicas = 0" \
    | grep -vF "ReadFromURL" | grep -oE "URL( × [0-9]+)?"

echo "--- which reads the early survivors without asking the generator past the limit"
# One thread, so that the `LIMIT` stops the pipeline before the single stream pulls its next address.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT url_glob_defer_path_filter"
$CLICKHOUSE_CLIENT --query "SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..2000}.tsv', 'TSV', 'x UInt64') WHERE ${filter} LIMIT 1 SETTINGS glob_expansion_max_elements = 2000, max_threads = 1, enable_parallel_replicas = 0"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT url_glob_defer_path_filter"

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses the S3 mock server on localhost:11111

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `_path` / `_file` predicate can prune the whole first batch of 1000 generated addresses. The
# number of streams must then follow the survivors of the later batches - prefetched while sizing
# the pipeline - not collapse to a single reader just because the first batch held no match.
# Pruned addresses are never fetched, so the queries below issue a handful of requests even though
# the pattern describes 1500 addresses.

prefix="${CLICKHOUSE_DATABASE}_multibatch"
for n in 1200 1201 1202 1203; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('http://localhost:11111/test/${prefix}_${n}.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT ${n}"
done

# `enable_parallel_replicas` would replace the `URL` source with a cluster read, hiding the
# pipeline shape this test pins.
echo "--- all four survivors of the second batch get their streams"
$CLICKHOUSE_CLIENT --query "EXPLAIN PIPELINE SELECT x FROM url('http://localhost:11111/test/${prefix}_{0..1499}.tsv', 'TSV', 'x UInt64') WHERE _file IN ('${prefix}_1200.tsv', '${prefix}_1201.tsv', '${prefix}_1202.tsv', '${prefix}_1203.tsv') SETTINGS glob_expansion_max_elements = 2000, max_threads = 4, enable_parallel_replicas = 0" \
    | grep -vF "ReadFromURL" | grep -oE "URL( × [0-9]+)?"

echo "--- and they are the addresses read"
$CLICKHOUSE_CLIENT --query "SELECT sum(x) FROM url('http://localhost:11111/test/${prefix}_{0..1499}.tsv', 'TSV', 'x UInt64') WHERE _file IN ('${prefix}_1200.tsv', '${prefix}_1201.tsv', '${prefix}_1202.tsv', '${prefix}_1203.tsv') SETTINGS glob_expansion_max_elements = 2000, max_threads = 4"

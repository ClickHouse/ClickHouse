#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (MinIO)

# Tests that parallel listing of a single big *flat* directory (s3_list_object_parallelism > 1, which
# splits the keyspace into contiguous sub-ranges listed concurrently) returns exactly the same files as
# the serial listing. A small s3_list_object_keys_size forces the directory to be truncated and split.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04340"

# Many files directly in one flat directory (no sub-directories). Write all of them with a single
# partitioned `INSERT` (one object per partition id) instead of 40 separate `INSERT` statements:
# under sanitizer builds each separate client round-trip costs a few seconds, so the loop alone could
# push the test past the per-test time limit, whereas a single query creates `data_01.csv` ... `data_40.csv`
# server-side in one pass. `leftPad(toString(x), 2, '0')` makes every partition id exactly two digits, so
# the files match the anchored `data_??.csv` glob below; digit-only partition ids pass the partition-value
# validation for `s3` writes.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${base}/data_{_partition_id}.csv', 'test', 'testtest', 'CSV', 'x UInt64') PARTITION BY leftPad(toString(x), 2, '0') SELECT number AS x FROM numbers(1, 40) SETTINGS s3_truncate_on_insert=1;"

# Match the files by an anchored glob (`data_??.csv`) rather than `*.csv`. Under the stress query fuzzer
# the directory can also hold sibling objects with mutated, non-UTF-8 names (e.g. `data\xef\xbf\xbd.csv`)
# produced from this test's harvested path literal; with `*.csv` the listing returns those and the read
# of their bodies fails with `No such key`, even with `s3_list_object_parallelism = 1` (i.e. unrelated to
# the feature under test). The anchored glob narrows the listing prefix to `.../04340/data_` and the regexp
# to the exact file shape, so a fuzzer artifact in the shared bucket is never listed or read and cannot
# derail this listing test. It still lists every `data_NN.csv` (truncated at `s3_list_object_keys_size`),
# which is what we compare between serial and parallel listing.
q="SELECT count(), uniqExact(_path), sum(sipHash64(_path)) FROM s3('${base}/data_??.csv', 'test', 'testtest', 'CSV', 'x UInt64') SETTINGS s3_list_object_keys_size=7"

serial=$($CLICKHOUSE_CLIENT -q "${q}, s3_list_object_parallelism=1")
parallel=$($CLICKHOUSE_CLIENT -q "${q}, s3_list_object_parallelism=8")
# A pathologically large parallelism must be clamped internally (not try to spawn a billion listing
# threads nor overflow the buffered-keys bound) and still return the same result as the serial listing.
clamped=$($CLICKHOUSE_CLIENT -q "${q}, s3_list_object_parallelism=1000000000")

if [ "$serial" == "$parallel" ] && [ "$serial" == "$clamped" ]; then
    # No duplicates iff count == uniqExact; print count for a deterministic, readable result.
    echo "flat listing serial==parallel OK, files=$(echo "$parallel" | cut -f1)"
else
    echo "MISMATCH serial=[${serial}] parallel=[${parallel}] clamped=[${clamped}]"
fi

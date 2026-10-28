#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-pre-distinct-null-keys.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# A set of `IN` whose subquery reads a table partitioned by the selected columns deduplicates each partition
# first. The deduplication drops the rows with a `NULL` key, which `IN` never matches, and hashes the other
# keys by their nested values, as the set does, so its hash method differs from the one of the `Nullable`
# keys. With a spill threshold for the set, it also estimates the memory of each chunk before hashing it,
# for the same method. Each case builds such a set in a fresh process from the numbers below 64 and `NULL`.
# It prints whether the deduplication runs, how many of the probes from 0 to 99 the set finds, and whether a
# deduplication switched to pass-through. A threshold of 1 GiB leaves the deduplication hashing. The optional
# `settings` are passed to the process.
run_case()
{
    local name="$1"
    local threshold="$2"
    local columns="$3"
    local partition="$4"
    local values="$5"
    local probe="$6"
    local subquery="$7"
    local settings="${8:-}"
    local in="SELECT count() FROM numbers(100) WHERE ${probe} IN (SELECT ${subquery} FROM t)"
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --max_threads 4 --force_creating_set_partitions_independently 1 \
        --max_bytes_before_external_set "${threshold}" --max_bytes_ratio_before_external_set 0 ${settings} --multiquery <<SQL
CREATE TABLE t (${columns}) ENGINE = MergeTree PARTITION BY ${partition} ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t SELECT ${values} FROM numbers(1000);
SELECT '${name}', (SELECT count() FROM (EXPLAIN PIPELINE ${in}) WHERE explain LIKE '%DistinctTransform%') > 0, (${in});
SELECT sumIf(value, event = 'DistinctTransformsSwitchedToPassThrough') > 0 FROM system.events;
SQL
}

# Hashed as `key32` instead of `nullable_keys128`.
run_case nullable_uint32 1073741824 'u Nullable(UInt32)' 'u % 8' \
    'if(number % 10 = 0, NULL, number % 64)' 'number' 'u'

# Hashed as `key_string` instead of `hashed`. The growth estimate of `key_string` reads the nested `String`
# column.
run_case nullable_string 1073741824 's Nullable(String)' 'cityHash64(s) % 8' \
    'if(number % 10 = 0, NULL, toString(number % 64))' 'toString(number)' 's'

# Hashed as `keys64` instead of `nullable_keys128`. Either part can be `NULL`, so the deduplication combines
# the null maps of both parts.
run_case two_nullable_parts 1073741824 'p Nullable(UInt32), q Nullable(UInt32)' 'p % 8' \
    'if(number % 10 = 0, NULL, number % 64), if(number % 10 = 5, NULL, number % 64)' '(number, number)' 'p, q'

# `LowCardinality(Nullable)` keys stay as they are, and their `NULL` rows are the rows of the `NULL` entry of
# the dictionary.
run_case low_cardinality_nullable 1073741824 'l LowCardinality(Nullable(String))' 'cityHash64(l) % 8' \
    'if(number % 10 = 0, NULL, toString(number % 64))' 'toString(number)' 'l'

# A threshold of 1 byte makes the deduplication pass rows through, and the set still drops the rows with a
# `NULL` key.
run_case pass_through 1 'u Nullable(UInt32)' 'u % 8' \
    'if(number % 10 = 0, NULL, number % 64)' 'number' 'u'

# With `transform_null_in = 1`, `IN` matches `NULL`, so the deduplication keeps the `NULL` key and hashes the
# keys with their null maps, as the set does. The probe of 99 is `NULL`, which the set finds besides the
# numbers below 64.
run_case transform_null_in 1073741824 'u Nullable(UInt32)' 'u % 8' \
    'if(number % 10 = 0, NULL, number % 64)' 'if(number = 99, NULL, number)' 'u' '--transform_null_in 1'

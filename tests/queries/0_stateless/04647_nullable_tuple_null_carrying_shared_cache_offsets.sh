#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Reading the whole `Nullable(Tuple(...))` column together with a null-carrying element subcolumn shares
# one substreams cache. The subcolumn read publishes the element's substreams into that cache and then
# marks the parent-NULL rows as NULL in them, which for Variant, Dynamic and LowCardinality physically
# removes those rows. The whole-column read then adopts the shortened substream from the cache while
# computing its own offsets from the unfiltered row count, so it reads shifted values (and trips
# `ColumnVariant::validateState` in debug and sanitizer builds).
#
# `clickhouse-local` is used because the whole-part fast path has to be off and every range must land in
# the same block, which needs a fixed reader configuration rather than a server's runtime one.

WORKDIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_DATABASE:?}_null_carrying_shared_cache"
rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"
trap 'rm -rf "$WORKDIR"' EXIT

run_local() {
    ${CLICKHOUSE_LOCAL} --path "$WORKDIR/$1" --multiquery "$2"
}

for i in 1 2 3 4 5 6; do
    mkdir -p "$WORKDIR/db$i"
done

echo '--- Variant element ---'
run_local db1 "
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE t (id UInt64, tup Nullable(Tuple(v Variant(UInt64))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t SELECT number, number % 2 ? NULL : tuple(number) FROM numbers(12);
SELECT id, tup.v, tup FROM t ORDER BY id;
SELECT id, tup, tup.v FROM t ORDER BY id;
SELECT id, tup.v, tup, tup.v.UInt64 FROM t ORDER BY id;
SELECT count(tup), count(tup.v), sum(tup.v.UInt64) FROM t;
"

echo '--- Variant element, more ranges ---'
run_local db2 "
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE t (id UInt64, tup Nullable(Tuple(v Variant(UInt64))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t SELECT number, number % 2 ? NULL : tuple(number) FROM numbers(40);
SELECT sum(tup.v.UInt64), count(tup), countIf(tup IS NULL) FROM t;
SELECT id, tup.v, tup FROM t WHERE id % 8 = 4 ORDER BY id;
SELECT id, tup.v, tup FROM t PREWHERE id % 4 >= 1 ORDER BY id LIMIT 8;
"

echo '--- Multi-variant element with its own NULLs ---'
run_local db3 "
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE t (id UInt64, tup Nullable(Tuple(v Variant(UInt64, String))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t SELECT number,
    number % 4 = 3 ? NULL : (number % 4 = 0 ? tuple(number::Variant(UInt64, String))
        : (number % 4 = 1 ? tuple(toString(number)::Variant(UInt64, String))
        : tuple(NULL::Variant(UInt64, String))))
FROM numbers(16);
SELECT id, tup.v, tup FROM t ORDER BY id;
SELECT id, tup.v, tup, tup.v.UInt64, tup.v.String FROM t ORDER BY id;
"

echo '--- Dynamic element ---'
run_local db4 "
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE t (id UInt64, tup Nullable(Tuple(a Dynamic)))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t SELECT number, number % 2 ? NULL : tuple(number::Dynamic) FROM numbers(12);
SELECT id, tup.a, tup FROM t ORDER BY id;
SELECT id, tup, tup.a, tup.a.UInt64 FROM t ORDER BY id;
"

echo '--- LowCardinality elements ---'
run_local db5 "
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE t_lc (id UInt64, tup Nullable(Tuple(s LowCardinality(Nullable(String)))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_lc SELECT number, number % 2 ? NULL : tuple(toString(number)) FROM numbers(12);
SELECT id, tup.s, tup FROM t_lc ORDER BY id;
CREATE TABLE t_lc_plain (id UInt64, tup Nullable(Tuple(p LowCardinality(String))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_lc_plain SELECT number, number % 2 ? NULL : tuple(toString(number)) FROM numbers(12);
SELECT id, tup.p, tup FROM t_lc_plain ORDER BY id;
"

echo '--- All element kinds, Wide and Compact ---'
run_local db6 "
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE t_wide (id UInt64, tup Nullable(Tuple(a Dynamic, v Variant(UInt64), s LowCardinality(Nullable(String)))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_wide SELECT number, number % 2 ? NULL : tuple(number::Dynamic, number, toString(number)) FROM numbers(24);
SELECT id, tup.a, tup.v, tup.s, tup FROM t_wide ORDER BY id;
SELECT id, tup, tup.a, tup.v, tup.s FROM t_wide ORDER BY id;
SELECT count(tup), count(tup.a), count(tup.v), count(tup.s) FROM t_wide;
-- Compact parts require adaptive granularity, so index_granularity_bytes stays at its default.
CREATE TABLE t_compact (id UInt64, tup Nullable(Tuple(a Dynamic, v Variant(UInt64), s LowCardinality(Nullable(String)))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 1000000000,
         ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_compact SELECT number, number % 2 ? NULL : tuple(number::Dynamic, number, toString(number)) FROM numbers(24);
SELECT id, tup.a, tup.v, tup.s, tup FROM t_compact ORDER BY id;
SELECT id, tup, tup.a, tup.v, tup.s FROM t_compact ORDER BY id;
"

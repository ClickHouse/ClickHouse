-- Exercises the COW-safe discriminator substream-cache path in `SerializationVariant`.
-- When a `Variant` column's discriminators are read with a partial-granule offset (rows_offset > 0)
-- after they were already placed into the substreams cache, the append and the in-place rows_offset
-- compaction must not mutate storage that is still referenced by the cache. A regression here surfaces
-- under ASan as `heap-use-after-free` / `double-free` during reads; on a normal build we assert that the
-- results stay correct. The tiny `index_granularity` with `index_granularity_bytes = 0` on a wide part,
-- combined with a `PREWHERE` that keeps the second row of each granule, drives the rows_offset > 0 reads.

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS t_variant_discr_cow;

CREATE TABLE t_variant_discr_cow (id UInt64, v Variant(UInt64, String))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_variant_discr_cow
SELECT number, multiIf(number % 3 = 0, number::UInt64, number % 3 = 1, 'str_' || toString(number), NULL)
FROM numbers(40);

-- Read the whole Variant together with its subcolumns while filtering to the second row of every
-- 2-row granule, so the surviving read starts at rows_offset = 1.
SELECT id, v, v.UInt64, v.String FROM t_variant_discr_cow PREWHERE id % 2 = 1 ORDER BY id;

-- Same partial-granule read, aggregated over the discriminators and a variant element.
SELECT
    countIf(variantType(v) = 'UInt64') AS n_uint,
    countIf(variantType(v) = 'String') AS n_str,
    countIf(variantType(v) = 'None') AS n_none,
    sum(v.UInt64) AS sum_uint
FROM t_variant_discr_cow PREWHERE id % 2 = 1;

-- Exercise the whole-column read across other partial-granule offsets once more (just the read path).
SELECT id, v, v.UInt64, v.String FROM t_variant_discr_cow PREWHERE id % 4 >= 1 ORDER BY id FORMAT Null;

DROP TABLE t_variant_discr_cow;

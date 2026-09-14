-- `partitioned_hash` builds the row store at the fill, as `parallel_hash` does: the fixed-width
-- right payload columns are packed row-wise and read back through the access indexes on the joined
-- output, on the RIGHT/FULL non-joined stream and on the grace spill drain. Checksums must match `hash`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET grace_hash_join_max_buckets = 1024;
SET enable_hash_join_row_store = 1;
-- The planner enables the row store only for an output estimate above this ratio; force it.
SET min_rows_ratio_for_hash_join_row_store = 0;

DROP TABLE IF EXISTS t_rs_build;
DROP TABLE IF EXISTS t_rs_build_small;
DROP TABLE IF EXISTS t_rs_probe;

CREATE TABLE t_rs_build
(
    k UInt64,
    kn Nullable(UInt64),
    a Int32,
    b Nullable(Int64),
    c UInt8,
    d Float64,
    e FixedString(16),
    f Nullable(FixedString(8)),
    g Decimal(18, 3),
    s String,
    h Nullable(FixedString(16)),
    i FixedString(32),
    j LowCardinality(String)
)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT
    number % 60000,
    if(number % 11 = 0, NULL, number % 60000),
    toInt32(number),
    if(number % 7 = 0, NULL, toInt64(number * 3)),
    toUInt8(number % 251),
    number / 7,
    reinterpret(number, 'FixedString(16)'),
    if(number % 5 = 0, NULL, toFixedString(toString(number % 1000), 8)),
    toDecimal64(number, 3),
    concat('s', toString(number)),
    if(number % 3 = 0, NULL, toFixedString(toString(number), 16)),
    toFixedString(concat('i', toString(number)), 32),
    toLowCardinality(concat('lc', toString(number % 97)))
FROM numbers(100000);

CREATE TABLE t_rs_probe (k UInt64, p UInt64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number, number + 1000000 FROM numbers(120000);

SELECT '-- inner all';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05139 inner all';

SELECT '-- inner all with the row store off: the columnar path on the same shape';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', enable_hash_join_row_store = 0, log_comment = '05139 inner all no store';
SELECT '-- inner all with the right table sorted by key: the store is still built and read';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', allow_experimental_join_right_table_sorting = 1;
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', allow_experimental_join_right_table_sorting = 1, log_comment = '05139 inner all sorted';
SELECT '-- inner all, first rows';
SELECT * FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k ORDER BY p.p, b.a LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT * FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k ORDER BY p.p, b.a LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';

SELECT '-- left all, defaults for the unmatched probe rows';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p LEFT JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p LEFT JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash';

SELECT '-- left all, first unmatched rows';
SELECT * FROM t_rs_probe AS p LEFT JOIN t_rs_build AS b ON p.k = b.k WHERE b.k = 0 ORDER BY p.p LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT * FROM t_rs_probe AS p LEFT JOIN t_rs_build AS b ON p.k = b.k WHERE b.k = 0 ORDER BY p.p LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';
SELECT '-- right all on a nullable key: non-joined rows plus the null-key rows';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p RIGHT JOIN t_rs_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p RIGHT JOIN t_rs_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05139 right all nullable';

SELECT '-- right all, first non-joined rows';
SELECT * FROM t_rs_probe AS p RIGHT JOIN t_rs_build AS b ON p.k = b.kn WHERE p.p = 0 ORDER BY b.a LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT * FROM t_rs_probe AS p RIGHT JOIN t_rs_build AS b ON p.k = b.kn WHERE p.p = 0 ORDER BY b.a LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';
SELECT '-- right all with the row store off';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p RIGHT JOIN t_rs_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'partitioned_hash', enable_hash_join_row_store = 0, log_comment = '05139 right all no store';
SELECT '-- full all';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p FULL JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p FULL JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05139 full all';

SELECT '-- full all, first rows of each side''s unmatched rows';
SELECT * FROM t_rs_probe AS p FULL JOIN t_rs_build AS b ON p.k = b.k WHERE p.p = 0 OR b.k = 0 ORDER BY p.p, b.a LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT * FROM t_rs_probe AS p FULL JOIN t_rs_build AS b ON p.k = b.k WHERE p.p = 0 OR b.k = 0 ORDER BY p.p, b.a LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';
SELECT '-- full all with the row store off';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p FULL JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', enable_hash_join_row_store = 0, log_comment = '05139 full all no store';
SELECT '-- left any: the eager output path, no row store';
SELECT count(), sum(cityHash64(p.p, b.k)), sum(b.a > 0)
FROM t_rs_probe AS p LEFT ANY JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k)), sum(b.a > 0)
FROM t_rs_probe AS p LEFT ANY JOIN t_rs_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', log_comment = '05139 left any';

SELECT '-- spill switch: the fill blocks are rebuilt from the row store for grace';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p INNER JOIN t_rs_build AS b ON p.k = b.k
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 200000, log_comment = '05139 spill';

SELECT '-- spill switch on the single fill thread: the stored blocks carry their row store into grace';
-- Below `parallel_hash_join_threshold` the one fill thread stores and inserts each block as it arrives; the
-- switch then drains the stored blocks themselves, the other drain.
CREATE TABLE t_rs_build_small ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM t_rs_build WHERE k < 30000;
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p FULL JOIN t_rs_build_small AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.k, ifNull(b.kn, 0), b.a, ifNull(b.b, 0), b.c, b.d, b.e, ifNull(b.f, ''), b.g, b.s, ifNull(b.h, ''), b.i, b.j))
FROM t_rs_probe AS p FULL JOIN t_rs_build_small AS b ON p.k = b.k
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 200000, log_comment = '05139 spill single fill';

SYSTEM FLUSH LOGS query_log;

SELECT '-- the row store was built (blocks > 0) where the shape admits it, and the spill queries switched to grace';
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinRowStoreBlocks'] > 0,
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05139 %'
ORDER BY log_comment;

DROP TABLE t_rs_build;
DROP TABLE t_rs_build_small;
DROP TABLE t_rs_probe;

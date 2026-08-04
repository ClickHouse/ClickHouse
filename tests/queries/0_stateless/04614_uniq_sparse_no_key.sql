-- Regression test for aggregation without key over sparse columns with
-- uniq, uniqExact and uniqHLL12. The optimized no-key path must handle
-- top-level sparse String / FixedString / IPv6 / numeric arguments exactly
-- like dense ones, both for plain aggregation without key and for
-- aggregation in order (which uses executeOnIntervalWithoutKey).
--
-- The results are compared against a dense copy of the same data, so the
-- check is robust to the approximate nature of uniq and uniqHLL12: for an
-- identical multiset of values the answer must not depend on whether the
-- column was stored sparse or dense.

DROP TABLE IF EXISTS t_uniq_sparse;
DROP TABLE IF EXISTS t_uniq_dense;

CREATE TABLE t_uniq_sparse
(
    g UInt64,
    num UInt64,
    s String,
    fs FixedString(8),
    ip IPv6
)
ENGINE = MergeTree ORDER BY (g, num)
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.01, min_bytes_for_wide_part = 0;

CREATE TABLE t_uniq_dense AS t_uniq_sparse
ENGINE = MergeTree ORDER BY (g, num)
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0, min_bytes_for_wide_part = 0;

INSERT INTO t_uniq_sparse
SELECT
    number % 8 AS g,
    if(number % 100 = 0, number, 0) AS num,
    if(number % 100 = 0, toString(number), '') AS s,
    if(number % 100 = 0, toFixedString(toString(number % 97), 8), toFixedString('', 8)) AS fs,
    if(number % 100 = 0, toIPv6('::' || toString(number % 97 + 1)), toIPv6('::')) AS ip
FROM numbers(200000);

INSERT INTO t_uniq_dense SELECT * FROM t_uniq_sparse;

-- The aggregate argument columns are sparse in the first table and dense in the second.
SELECT column, any(serialization_kind)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_uniq_sparse' AND active AND column IN ('num', 's', 'fs', 'ip')
GROUP BY column ORDER BY column;
SELECT column, any(serialization_kind)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_uniq_dense' AND active AND column IN ('num', 's', 'fs', 'ip')
GROUP BY column ORDER BY column;

-- Aggregation without key (plain path): sparse must equal dense.
SELECT
    (SELECT uniq(num) FROM t_uniq_sparse) = (SELECT uniq(num) FROM t_uniq_dense),
    (SELECT uniqExact(num) FROM t_uniq_sparse) = (SELECT uniqExact(num) FROM t_uniq_dense),
    (SELECT uniqHLL12(num) FROM t_uniq_sparse) = (SELECT uniqHLL12(num) FROM t_uniq_dense),
    (SELECT uniqCombined(num) FROM t_uniq_sparse) = (SELECT uniqCombined(num) FROM t_uniq_dense),
    (SELECT uniqCombined64(num) FROM t_uniq_sparse) = (SELECT uniqCombined64(num) FROM t_uniq_dense),
    (SELECT uniq(s) FROM t_uniq_sparse) = (SELECT uniq(s) FROM t_uniq_dense),
    (SELECT uniqExact(s) FROM t_uniq_sparse) = (SELECT uniqExact(s) FROM t_uniq_dense),
    (SELECT uniqHLL12(s) FROM t_uniq_sparse) = (SELECT uniqHLL12(s) FROM t_uniq_dense),
    (SELECT uniqCombined(s) FROM t_uniq_sparse) = (SELECT uniqCombined(s) FROM t_uniq_dense),
    (SELECT uniqCombined64(s) FROM t_uniq_sparse) = (SELECT uniqCombined64(s) FROM t_uniq_dense),
    (SELECT uniq(fs) FROM t_uniq_sparse) = (SELECT uniq(fs) FROM t_uniq_dense),
    (SELECT uniqExact(fs) FROM t_uniq_sparse) = (SELECT uniqExact(fs) FROM t_uniq_dense),
    (SELECT uniqHLL12(fs) FROM t_uniq_sparse) = (SELECT uniqHLL12(fs) FROM t_uniq_dense),
    (SELECT uniqCombined(fs) FROM t_uniq_sparse) = (SELECT uniqCombined(fs) FROM t_uniq_dense),
    (SELECT uniqCombined64(fs) FROM t_uniq_sparse) = (SELECT uniqCombined64(fs) FROM t_uniq_dense),
    (SELECT uniq(ip) FROM t_uniq_sparse) = (SELECT uniq(ip) FROM t_uniq_dense),
    (SELECT uniqExact(ip) FROM t_uniq_sparse) = (SELECT uniqExact(ip) FROM t_uniq_dense),
    (SELECT uniqHLL12(ip) FROM t_uniq_sparse) = (SELECT uniqHLL12(ip) FROM t_uniq_dense),
    (SELECT uniqCombined(ip) FROM t_uniq_sparse) = (SELECT uniqCombined(ip) FROM t_uniq_dense),
    (SELECT uniqCombined64(ip) FROM t_uniq_sparse) = (SELECT uniqCombined64(ip) FROM t_uniq_dense);

-- Aggregation in order over a sort-prefix key (uses executeOnIntervalWithoutKey):
-- the whole set of per-group result tuples must be identical for sparse and dense.
SELECT
(
    SELECT arraySort(groupArray(t)) FROM
    (
        SELECT (g, uniq(num), uniqExact(num), uniqHLL12(num), uniqCombined(num), uniqCombined64(num),
                   uniq(s), uniqExact(s), uniqHLL12(s), uniqCombined(s), uniqCombined64(s),
                   uniq(fs), uniqExact(fs), uniqHLL12(fs), uniqCombined(fs), uniqCombined64(fs),
                   uniq(ip), uniqExact(ip), uniqHLL12(ip), uniqCombined(ip), uniqCombined64(ip)) AS t
        FROM t_uniq_sparse GROUP BY g
        SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1
    )
)
=
(
    SELECT arraySort(groupArray(t)) FROM
    (
        SELECT (g, uniq(num), uniqExact(num), uniqHLL12(num), uniqCombined(num), uniqCombined64(num),
                   uniq(s), uniqExact(s), uniqHLL12(s), uniqCombined(s), uniqCombined64(s),
                   uniq(fs), uniqExact(fs), uniqHLL12(fs), uniqCombined(fs), uniqCombined64(fs),
                   uniq(ip), uniqExact(ip), uniqHLL12(ip), uniqCombined(ip), uniqCombined64(ip)) AS t
        FROM t_uniq_dense GROUP BY g
        SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1
    )
);

DROP TABLE t_uniq_sparse;
DROP TABLE t_uniq_dense;

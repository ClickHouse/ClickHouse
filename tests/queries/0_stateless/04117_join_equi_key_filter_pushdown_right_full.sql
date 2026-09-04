-- Tags: no-parallel-replicas
-- no-parallel-replicas: the granule assertions describe the local `MergeTree` read, which parallel
-- replicas replace, and the `RIGHT JOIN` shapes below hit the unrelated logical error of
-- https://github.com/ClickHouse/ClickHouse/issues/113292 there.

-- Equi-key `WHERE` predicates must reach the opposite `MergeTree` input of a `RIGHT JOIN` or a plain
-- `INNER JOIN` as an index condition, including when the two join keys differ in type.
--
-- Analyzer only. Under `enable_analyzer = 0` a `USING` key is renamed to `<table>.<key>` in the right
-- input header while the `JOIN` output keeps the bare name, so the equivalence maps are keyed by a name
-- the filter never references and nothing is pushed. That path is left as is.
--
-- Nothing may be pushed through a `FULL JOIN`: a dropped row only becomes a defaulted unmatched row, so
-- a predicate on that default both admits and discards rows wrongly. Those cases assert results only.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS mt;
CREATE TABLE mt (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO mt SELECT number FROM numbers(1000000);

SELECT 'RIGHT JOIN USING, equi-key WHERE, matched types: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt AS l RIGHT JOIN (SELECT toUInt64(1) AS k) AS r USING (k) WHERE k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT JOIN USING, equi-key WHERE, UInt8/UInt64 mismatch: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT JOIN USING, equi-key WHERE: result';
SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'RIGHT ALL JOIN ON, equi-key WHERE: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT l.k FROM mt AS l RIGHT JOIN (SELECT toUInt64(1) AS k) AS r ON l.k = r.k WHERE r.k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT ANTI JOIN USING, equi-key WHERE: correctness preserved';
SELECT k FROM mt AS l RIGHT ANTI JOIN (SELECT toUInt64(0) AS k UNION ALL SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 1000000000 ORDER BY k;

-- A substitution has to carry the type the replaced name has in the `JOIN` output, and the only type
-- reachable by casting the opposite key is the least supertype of the two. `join_use_nulls` widens the
-- output past that supertype, so nothing is substituted and the predicate stays above the `JOIN`.
SET join_use_nulls = 1;

SELECT 'RIGHT JOIN USING, equi-key WHERE, join_use_nulls: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT JOIN USING, equi-key WHERE, join_use_nulls: result';
SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'RIGHT JOIN ON, equi-key WHERE, join_use_nulls: unmatched right row keeps its NULL left side';
SELECT l.k, r.k FROM mt AS l RIGHT JOIN (SELECT toUInt64(1000000000) AS k) AS r ON l.k = r.k WHERE r.k = 1000000000 ORDER BY 1, 2;

SET join_use_nulls = 0;

SELECT 'FULL JOIN USING, equi-key WHERE: result (matched row)';
SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1) AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: result (right-only contributing row preserved)';
SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 1000000000 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: left-only rows still reachable via filter';
SELECT count() FROM (
    SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 7
);

DROP TABLE mt;

DROP TABLE IF EXISTS s1;
DROP TABLE IF EXISTS s2;
CREATE TABLE s1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s2 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO s1 VALUES (5);
INSERT INTO s2 VALUES (3);

-- Pushing this would drop the only left row and lose the `(5, 0)` unmatched row it produces.
SELECT 'FULL JOIN, side-qualified equi-key predicate: unmatched rows preserved';
SELECT lhs.id, rhs.id FROM s1 AS lhs FULL JOIN s2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 5 ORDER BY 1, 2;

DROP TABLE s1;
DROP TABLE s2;

DROP TABLE IF EXISTS u1;
DROP TABLE IF EXISTS u2;
CREATE TABLE u1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE u2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO u1 VALUES (1, 'Value_1'), (2, 'Value_2');
INSERT INTO u2 VALUES (2, 'Value_2'), (3, 'Value_3');

-- Opposite direction: pushing these would let the two defaulted unmatched rows escape.
SELECT 'FULL JOIN, side-qualified equi-key predicates on both sides: only the matched row survives';
SELECT * FROM u1 AS lhs FULL JOIN u2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0 ORDER BY 1, 3;

DROP TABLE u1;
DROP TABLE u2;

-- A `USING` supertype that is wider than one or both inputs. The replacement is then the same `CAST`
-- the `JOIN` applies to its key, so the left input is still read through the primary key.

DROP TABLE IF EXISTS mt_i32;
CREATE TABLE mt_i32 (k Int32) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO mt_i32 SELECT number FROM numbers(1000000);

SELECT 'RIGHT JOIN USING, Int32 / UInt32 keys widened to Int64: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::UInt32 AS k) AS r USING (k) WHERE k = 1
) WHERE explain LIKE '%Granules: 1/%';

SELECT 'RIGHT JOIN USING, Int32 / UInt32 keys widened to Int64: result';
SELECT k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::UInt32 AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'RIGHT JOIN USING, Int32 / Int64 keys widened to Int64: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::Int64 AS k) AS r USING (k) WHERE k = 1
) WHERE explain LIKE '%Granules: 1/%';

SELECT 'RIGHT JOIN USING, Int32 / Int64 keys widened to Int64: result';
SELECT k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::Int64 AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'RIGHT JOIN ON, cross-type equi-key: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT l.k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::Int64 AS k) AS r ON l.k = r.k WHERE r.k = 1
) WHERE explain LIKE '%Granules: 1/%';

SELECT 'RIGHT JOIN USING, cross-type: unmatched right row preserved';
SELECT k FROM mt_i32 AS l RIGHT JOIN (SELECT 5::UInt32 AS k UNION ALL SELECT 1000000000::UInt32 AS k) AS r USING (k) WHERE k >= 5 ORDER BY k;

-- The opposite direction would need a narrowing `CAST`, which is not a substitution, so the predicate
-- stays above the `JOIN` and the wider key value survives it.
SELECT 'RIGHT JOIN ON, predicate on the wider side: no narrowing substitution';
SELECT count() FROM (
    SELECT l.k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::Int64 AS k) AS r ON l.k = r.k WHERE l.k = 1
);

SET join_use_nulls = 1;

-- A `RIGHT JOIN` takes its `USING` key from the right input, which `join_use_nulls` does not widen, so
-- the output type is still the plain supertype and the substitution stays exact: the left input keeps
-- pruning. The `EXPLAIN` assertion pins that, because the result alone would also hold if the pushdown
-- silently stopped happening.
SELECT 'RIGHT JOIN USING, cross-type, join_use_nulls: left MergeTree still prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::UInt32 AS k) AS r USING (k) WHERE k = 1
) WHERE explain LIKE '%Granules: 1/%';

SELECT 'RIGHT JOIN USING, cross-type, join_use_nulls: result';
SELECT k FROM mt_i32 AS l RIGHT JOIN (SELECT 1::UInt32 AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SET join_use_nulls = 0;

-- The safety boundary itself: a `FULL JOIN` turns a dropped row into a defaulted unmatched one, so
-- neither side may be filtered, whatever the substitution would allow. Asserted on the plan, not only
-- on the rows, so that a later change re-enabling a side is caught even when the value survives it.
DROP TABLE IF EXISTS mt_u32;
CREATE TABLE mt_u32 (k UInt32) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO mt_u32 SELECT number FROM numbers(1000000);

SELECT 'FULL JOIN USING, cross-type: neither MergeTree prunes granules';
SELECT count() = 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt_i32 AS l FULL JOIN mt_u32 AS r USING (k) WHERE k = 1
) WHERE explain LIKE '%Granules: 1/%';

SET join_use_nulls = 1;
SELECT 'FULL JOIN USING, cross-type, join_use_nulls: neither MergeTree prunes granules';
SELECT count() = 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt_i32 AS l FULL JOIN mt_u32 AS r USING (k) WHERE k = 1
) WHERE explain LIKE '%Granules: 1/%';
SET join_use_nulls = 0;

DROP TABLE mt_u32;
DROP TABLE mt_i32;

-- A plain `INNER JOIN` leaves both inputs open to a pushed-down filter, so a cross-type equi-key must
-- reach the opposite input there too. The side ordered by `tuple()` can never prune, so a read that
-- prunes granules is unambiguously the other one; the assertion compares the two granule counts rather
-- than matching a literal total, which depends on the part layout.

DROP TABLE IF EXISTS inner_d32;
DROP TABLE IF EXISTS inner_d;
DROP TABLE IF EXISTS inner_d32_key;
CREATE TABLE inner_d32     (a Date32) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE inner_d       (b Date)   ENGINE = MergeTree ORDER BY b
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE inner_d32_key (a Date32) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO inner_d32     SELECT toDate32('2020-01-01') + number FROM numbers(40000);
INSERT INTO inner_d       SELECT toDate('2020-01-01')   + number FROM numbers(40000);
INSERT INTO inner_d32_key SELECT toDate32('2020-01-01') + number FROM numbers(40000);

SELECT 'INNER JOIN ON, cross-type equi-key, predicate on the wider key: right MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM inner_d32 AS l INNER JOIN inner_d AS r ON l.a = r.b
    WHERE l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
) WHERE toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) < toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'));

SELECT 'INNER JOIN ON, cross-type equi-key, wider key on the right: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM inner_d AS l INNER JOIN inner_d32 AS r ON l.b = r.a
    WHERE r.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
) WHERE toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) < toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'));

SELECT 'INNER JOIN ON, matched types: right MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM inner_d32 AS l INNER JOIN inner_d32_key AS r ON l.a = r.a
    WHERE l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
) WHERE toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) < toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'));

-- Only a conjunct that reads the representation of a substituted key is unsafe, so a safe conjunct on
-- that same key still reaches the opposite input when the two sit side by side. `isConstant(l.a) = 0`
-- holds for every row of this read, so it constrains nothing and the granule counts describe the range.

SELECT 'INNER JOIN ON, cross-type equi-key beside a predicate reading constness: right MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM inner_d32 AS l INNER JOIN inner_d AS r ON l.a = r.b
    WHERE isConstant(l.a) = 0 AND l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
) WHERE toUInt64OrZero(extract(explain, 'Granules: ([0-9]+)/')) < toUInt64OrZero(extract(explain, 'Granules: [0-9]+/([0-9]+)'));

-- A lambda keeps its body in an inner expression that a walk over the outer nodes never reaches, so both
-- properties above are asserted twice: once where the body satisfies them and once where it hides a
-- violation. `% 1` keeps `rand` from changing the key's value, leaving the body as the only difference,
-- and the two spellings render identically in the plan, so only the pushed filter tells them apart.

SELECT 'INNER JOIN ON, cross-type equi-key with a deterministic lambda body: the key reaches the right input';
SELECT countIf(explain ILIKE '%Filter column%arrayMax%') = 1 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM inner_d32 AS l INNER JOIN inner_d AS r ON l.a = arrayMax(arrayMap(z -> z, [r.b]))
    WHERE l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
);

SELECT 'INNER JOIN ON, cross-type equi-key whose lambda body is not stable within the query: the key does not';
SELECT countIf(explain ILIKE '%Filter column%arrayMax%') = 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM inner_d32 AS l INNER JOIN inner_d AS r ON l.a = arrayMax(arrayMap(z -> z + (rand(z) % 1), [r.b]))
    WHERE l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
);

-- A both-streams conjunct is evaluated once per side and dropped from the post-join filter, so an unstable
-- body would be drawn twice and the two draws compared. The body sits beside the key here rather than above
-- it, which is the position the conjunct's own walk cannot reach. `% 1` keeps the conjunct true for every
-- row, and the array is materialized because a constant one folds the whole call away before the plan.

SELECT 'INNER JOIN ON, conjunct with a deterministic lambda body beside the equi-key: the conjunct is pushed';
SELECT countIf(explain ILIKE '%arrayExists%CAST(b AS Date32)%') = 1 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM inner_d32 AS l INNER JOIN inner_d AS r ON l.a = r.b
    WHERE arrayExists(y -> y % 1 = 0, materialize([1])) = (l.a > toDate32('1900-01-01'))
      AND l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
);

SELECT 'INNER JOIN ON, conjunct with an unstable lambda body beside the equi-key: the conjunct is not pushed';
SELECT countIf(explain ILIKE '%arrayExists%CAST(b AS Date32)%') = 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM inner_d32 AS l INNER JOIN inner_d AS r ON l.a = r.b
    WHERE arrayExists(y -> rand(y) % 1 = 0, materialize([1])) = (l.a > toDate32('1900-01-01'))
      AND l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03')
);

SELECT 'INNER JOIN ON, cross-type equi-key: result';
SELECT r.b FROM inner_d32 AS l INNER JOIN inner_d AS r ON l.a = r.b
WHERE l.a BETWEEN toDate32('2020-06-01') AND toDate32('2020-06-03') ORDER BY 1;

DROP TABLE inner_d32;
DROP TABLE inner_d;
DROP TABLE inner_d32_key;

-- A key that is not stable within the query is not substitutable: the pushed-down filter computes it and
-- the JOIN computes it again, so the two comparisons would see different values and matching rows would
-- be dropped. `* 0` keeps the value at the row number while making the key an expression of the right
-- input, and `max_threads` is pinned because `rowNumberInAllBlocks` numbers each stream from zero.

DROP TABLE IF EXISTS inner_nd_wide;
DROP TABLE IF EXISTS inner_nd_narrow;
CREATE TABLE inner_nd_wide   (a Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE inner_nd_narrow (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO inner_nd_wide   SELECT number FROM numbers(10);
INSERT INTO inner_nd_narrow SELECT number FROM numbers(10);

SELECT 'INNER JOIN ON, cross-type equi-key that is not stable within the query: result';
SELECT l.a FROM inner_nd_wide AS l INNER JOIN inner_nd_narrow AS r
    ON l.a = toInt32(rowNumberInAllBlocks() + r.x * 0)
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1
SETTINGS max_threads = 1;

DROP TABLE inner_nd_wide;
DROP TABLE inner_nd_narrow;

-- A key that changes the number of rows is not substitutable either: the pushed-down filter expands the
-- rows and the JOIN expands them again, so a match is reported once per extra copy. The array repeats its
-- element so that the second expansion changes the result and not just an intermediate row count.

DROP TABLE IF EXISTS inner_aj_wide;
DROP TABLE IF EXISTS inner_aj_narrow;
CREATE TABLE inner_aj_wide   (a Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE inner_aj_narrow (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO inner_aj_wide   SELECT number FROM numbers(10);
INSERT INTO inner_aj_narrow SELECT number FROM numbers(10);

SELECT 'INNER JOIN ON, cross-type equi-key that changes the number of rows: result';
SELECT l.a FROM inner_aj_wide AS l INNER JOIN inner_aj_narrow AS r
    ON l.a = toInt32(arrayJoin([r.x, r.x]))
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1;

DROP TABLE inner_aj_wide;
DROP TABLE inner_aj_narrow;

-- A float key can be join-equal while bit-different: `-0.0` and `+0.0` are equal to the comparison a
-- merge-based algorithm joins on, so substituting one side's key for the other would let a bit-sensitive
-- predicate drop a matching row. A nested float is no different, and `Array` is used for the second shape
-- because the wrapper-stripping predicates that cover `Nullable` do not look inside it. `join_algorithm` is
-- pinned at query level because the hash algorithms hash the key bitwise and never match the pair at all.
-- The flat shape uses `LEFT JOIN`, whose cross-type registration is older than the plain `INNER` one, so
-- there a dropped right row surfaces as a defaulted `r.x`; the nested shape stays on `INNER`.

DROP TABLE IF EXISTS left_f64;
DROP TABLE IF EXISTS left_f32;
CREATE TABLE left_f64 (a Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE left_f32 (x Float32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO left_f64 VALUES (0.0), (3.5);
INSERT INTO left_f32 VALUES (-0.0), (3.5);

SELECT 'LEFT JOIN ON, cross-type float equi-key: result';
SELECT l.a, r.x FROM left_f64 AS l LEFT JOIN left_f32 AS r ON l.a = r.x
WHERE reinterpretAsUInt64(l.a) = 0 ORDER BY 1
SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE left_f64;
DROP TABLE left_f32;

DROP TABLE IF EXISTS inner_af64;
DROP TABLE IF EXISTS inner_af32;
CREATE TABLE inner_af64 (a Array(Float64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE inner_af32 (x Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO inner_af64 VALUES ([0.0]), ([3.5]);
INSERT INTO inner_af32 VALUES ([-0.0]), ([3.5]);

SELECT 'INNER JOIN ON, cross-type equi-key with a nested float: result';
SELECT l.a, r.x FROM inner_af64 AS l INNER JOIN inner_af32 AS r ON l.a = r.x
WHERE reinterpretAsUInt64(l.a[1]) = 0 ORDER BY 1
SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE inner_af64;
DROP TABLE inner_af32;

-- `Dynamic` does not describe its runtime alternatives in the static type, so a float inside one is invisible
-- to a walk over the type and the type is declined outright. A `Dynamic` join key is rejected unless
-- `allow_dynamic_type_in_join_keys` is set, so the arm sets it next to the algorithm at query level.

DROP TABLE IF EXISTS inner_dyn;
DROP TABLE IF EXISTS inner_dyn_f32;
CREATE TABLE inner_dyn     (a Dynamic) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE inner_dyn_f32 (x Float32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO inner_dyn     VALUES (toFloat32(0.0)), (toFloat32(3.5));
INSERT INTO inner_dyn_f32 VALUES (-0.0), (3.5);

SELECT 'INNER JOIN ON, cross-type equi-key with a float inside a Dynamic: result';
SELECT l.a, r.x FROM inner_dyn AS l INNER JOIN inner_dyn_f32 AS r ON l.a = r.x
WHERE reinterpretAsUInt32(assumeNotNull(dynamicElement(l.a, 'Float32'))) = 0 ORDER BY r.x
SETTINGS allow_dynamic_type_in_join_keys = 1, join_algorithm = 'full_sorting_merge';

DROP TABLE inner_dyn;
DROP TABLE inner_dyn_f32;

-- Which `JSON` paths get their own subcolumn is a property of the column, not of the type: two earlier
-- rows fill both dynamic slots on the left, so its `x` lands in shared data while the right column keeps
-- `x` dynamic. What disagrees between the sides is `JSONDynamicPaths`, which reads that placement rather
-- than the value; the values themselves are equal under every join comparison, so unlike the float arms
-- above this one needs no algorithm pinned.

DROP TABLE IF EXISTS inner_json_shared;
DROP TABLE IF EXISTS inner_json_dyn;
CREATE TABLE inner_json_shared (a JSON(max_dynamic_paths = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE inner_json_dyn    (x JSON(max_dynamic_paths = 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO inner_json_shared VALUES ('{"p":1}'), ('{"q":1}'), ('{"x":1}');
INSERT INTO inner_json_dyn VALUES ('{"x":1}');

SELECT 'INNER JOIN ON, cross-type equi-key with a JSON path in shared data on one side: result';
SELECT l.a, r.x FROM inner_json_shared AS l INNER JOIN inner_json_dyn AS r ON l.a = r.x
WHERE NOT has(JSONDynamicPaths(l.a), 'x') ORDER BY 1;

DROP TABLE inner_json_shared;
DROP TABLE inner_json_dyn;

-- A substituted key must not feed a predicate that reads representation rather than value: `isConstant`
-- answers differently for the same value depending on constness, and the narrower key here is constant
-- while the wider key it replaces is not, so pushing the predicate would drop the matching row.

DROP TABLE IF EXISTS inner_ic_wide;
CREATE TABLE inner_ic_wide (a Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO inner_ic_wide VALUES (0);

SELECT 'INNER JOIN ON, cross-type equi-key under a predicate reading constness: result';
SELECT l.a FROM inner_ic_wide AS l INNER JOIN (SELECT toInt32(0) AS b) AS r ON l.a = r.b
WHERE isConstant(l.a) = 0 ORDER BY 1;

-- Reading it from inside a lambda body is the same predicate: the formal parameter comes from a constant
-- array here, so the substituted key is constant on both sides of `if` and the whole read turns constant.

SELECT 'INNER JOIN ON, equi-key under a predicate reading constness inside a lambda body: result';
SELECT l.a FROM inner_ic_wide AS l INNER JOIN (SELECT toInt32(0) AS b) AS r ON l.a = r.b
WHERE arrayExists(y -> isConstant(if(y, l.a, l.a)) = 0, [1]) ORDER BY 1;

DROP TABLE inner_ic_wide;

-- Results of `ORDER BY ... LIMIT` over an `ARRAY JOIN` must not depend on
-- `query_plan_top_k_through_array_join`. Every query below is run twice, once with the
-- optimization off and once with it on, and the two outputs must match.

DROP TABLE IF EXISTS t_aj;

-- No primary key: `ORDER BY x` cannot be satisfied by reading in order, so the rewrite is not
-- skipped in favour of `optimizeReadInOrder`.
CREATE TABLE t_aj
(
    x UInt64,
    s String,
    arr Array(UInt32),
    arr2 Array(UInt32),
    m Map(String, UInt32)
)
ENGINE = MergeTree ORDER BY tuple();

-- `x` is unique, so the ordering of `x` values in any top-N is unambiguous.
-- Every second row has all-empty containers: an inner ARRAY JOIN drops those rows, a
-- LEFT ARRAY JOIN keeps them with one default element. The non-empty rows all have
-- exactly 2 elements, so a LIMIT that is a multiple of 2 never splits an expanded run
-- and the full result rows are deterministic.
INSERT INTO t_aj
SELECT
    number,
    toString(number),
    if(number % 2 = 0, [toUInt32(number), toUInt32(number + 1000)], []),
    if(number % 2 = 0, [toUInt32(number * 2), toUInt32(number * 3)], []),
    if(number % 2 = 0, map('a', toUInt32(number), 'b', toUInt32(number + 1)), CAST(map(), 'Map(String, UInt32)'))
FROM numbers(100);

SELECT '-- inner ARRAY JOIN';
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 10) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 10) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- LEFT ARRAY JOIN';
SELECT x FROM (SELECT x FROM t_aj LEFT ARRAY JOIN arr ORDER BY x LIMIT 10) ORDER BY x SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x FROM (SELECT x FROM t_aj LEFT ARRAY JOIN arr ORDER BY x LIMIT 10) ORDER BY x SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- descending order';
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY x DESC LIMIT 10) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY x DESC LIMIT 10) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- LIMIT with OFFSET';
SELECT x FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 6 OFFSET 4) ORDER BY x SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 6 OFFSET 4) ORDER BY x SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- several aligned joined arrays';
SELECT x, e, e2 FROM (SELECT x, arr AS e, arr2 AS e2 FROM t_aj ARRAY JOIN arr, arr2 ORDER BY x LIMIT 10) ORDER BY x, e, e2 SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e, e2 FROM (SELECT x, arr AS e, arr2 AS e2 FROM t_aj ARRAY JOIN arr, arr2 ORDER BY x LIMIT 10) ORDER BY x, e, e2 SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- Map';
SELECT x, e FROM (SELECT x, m AS e FROM t_aj ARRAY JOIN m ORDER BY x LIMIT 10) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, m AS e FROM t_aj ARRAY JOIN m ORDER BY x LIMIT 10) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- computed sort key';
SELECT y, e FROM (SELECT 1000000 - x AS y, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY y LIMIT 10) ORDER BY y, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT y, e FROM (SELECT 1000000 - x AS y, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY y LIMIT 10) ORDER BY y, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- chained ARRAY JOINs';
SELECT x, e, e2 FROM (SELECT x, arr AS e, arr2 AS e2 FROM t_aj ARRAY JOIN arr ARRAY JOIN arr2 ORDER BY x LIMIT 12) ORDER BY x, e, e2 SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e, e2 FROM (SELECT x, arr AS e, arr2 AS e2 FROM t_aj ARRAY JOIN arr ARRAY JOIN arr2 ORDER BY x LIMIT 12) ORDER BY x, e, e2 SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- LIMIT larger than the whole result';
SELECT count(), sum(x) FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 1000) SETTINGS query_plan_top_k_through_array_join = 0;
SELECT count(), sum(x) FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 1000) SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- ORDER BY a joined column must not be rewritten';
SELECT e FROM (SELECT arr AS e FROM t_aj ARRAY JOIN arr ORDER BY arr LIMIT 5) ORDER BY e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT e FROM (SELECT arr AS e FROM t_aj ARRAY JOIN arr ORDER BY arr LIMIT 5) ORDER BY e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- ORDER BY an expression over a joined column must not be rewritten';
SELECT e FROM (SELECT arr AS e FROM t_aj ARRAY JOIN arr ORDER BY arr % 7, arr LIMIT 5) ORDER BY e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT e FROM (SELECT arr AS e FROM t_aj ARRAY JOIN arr ORDER BY arr % 7, arr LIMIT 5) ORDER BY e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- WITH TIES';
SELECT count() FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY intDiv(x, 10) LIMIT 3 WITH TIES) SETTINGS query_plan_top_k_through_array_join = 0;
SELECT count() FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY intDiv(x, 10) LIMIT 3 WITH TIES) SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- WITH FILL';
SELECT x FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY x WITH FILL STEP 1 LIMIT 7) ORDER BY x SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x FROM (SELECT x FROM t_aj ARRAY JOIN arr ORDER BY x WITH FILL STEP 1 LIMIT 7) ORDER BY x SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- a WHERE on the joined column stays between the sort and the ARRAY JOIN';
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr WHERE arr > 500 ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr WHERE arr > 500 ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- a WHERE that does not reference the joined column is pushed below the ARRAY JOIN';
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr WHERE x > 20 ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr WHERE x > 20 ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- LIMIT BY between the sort and the ARRAY JOIN';
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 1 BY x LIMIT 5) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj ARRAY JOIN arr ORDER BY x LIMIT 1 BY x LIMIT 5) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- constant array';
SELECT x, e FROM (SELECT x, e FROM t_aj ARRAY JOIN [1, 2] AS e ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, e FROM t_aj ARRAY JOIN [1, 2] AS e ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- constant empty array';
SELECT count() FROM (SELECT x FROM t_aj ARRAY JOIN CAST([], 'Array(UInt32)') AS e ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 0;
SELECT count() FROM (SELECT x FROM t_aj ARRAY JOIN CAST([], 'Array(UInt32)') AS e ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 1;

DROP TABLE t_aj;

-- Every array is empty, but in a real column, so the guard cannot be folded away: it must
-- discard every row and the inner ARRAY JOIN must produce nothing.
DROP TABLE IF EXISTS t_aj_all_empty;

CREATE TABLE t_aj_all_empty (x UInt64, arr Array(UInt32))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_aj_all_empty SELECT number, [] FROM numbers(100);

SELECT '-- every array empty, inner';
SELECT count() FROM (SELECT x FROM t_aj_all_empty ARRAY JOIN arr ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 0;
SELECT count() FROM (SELECT x FROM t_aj_all_empty ARRAY JOIN arr ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- every array empty, LEFT';
SELECT count(), sum(x) FROM (SELECT x FROM t_aj_all_empty LEFT ARRAY JOIN arr ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 0;
SELECT count(), sum(x) FROM (SELECT x FROM t_aj_all_empty LEFT ARRAY JOIN arr ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 1;

DROP TABLE t_aj_all_empty;

-- Ragged array sizes: which elements of the run at the LIMIT boundary are returned is not
-- specified, but the multiset of sort-key values in the top-N is, so aggregate over it only.
DROP TABLE IF EXISTS t_aj_ragged;

CREATE TABLE t_aj_ragged (x UInt64, arr Array(UInt32), arr2 Array(UInt32))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_aj_ragged
SELECT number, range(number % 5), range(number % 3)
FROM numbers(200);

SELECT '-- ragged, inner';
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged ARRAY JOIN arr ORDER BY x LIMIT 17) SETTINGS query_plan_top_k_through_array_join = 0;
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged ARRAY JOIN arr ORDER BY x LIMIT 17) SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- ragged, LEFT';
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged LEFT ARRAY JOIN arr ORDER BY x LIMIT 17) SETTINGS query_plan_top_k_through_array_join = 0;
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged LEFT ARRAY JOIN arr ORDER BY x LIMIT 17) SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- ragged, unaligned, inner';
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged ARRAY JOIN arr, arr2 ORDER BY x LIMIT 17) SETTINGS enable_unaligned_array_join = 1, query_plan_top_k_through_array_join = 0;
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged ARRAY JOIN arr, arr2 ORDER BY x LIMIT 17) SETTINGS enable_unaligned_array_join = 1, query_plan_top_k_through_array_join = 1;

SELECT '-- ragged, unaligned, LEFT';
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged LEFT ARRAY JOIN arr, arr2 ORDER BY x LIMIT 17) SETTINGS enable_unaligned_array_join = 1, query_plan_top_k_through_array_join = 0;
SELECT count(), sum(x), min(x), max(x) FROM (SELECT x FROM t_aj_ragged LEFT ARRAY JOIN arr, arr2 ORDER BY x LIMIT 17) SETTINGS enable_unaligned_array_join = 1, query_plan_top_k_through_array_join = 1;

DROP TABLE t_aj_ragged;

-- Arrays of unequal size under an aligned inner ARRAY JOIN must still throw. The emptiness
-- guard spans every joined column (`length(arr) > 0 OR length(arr2) > 0`) rather than just the
-- first one, precisely so that a row like `([], [1])` reaches the step instead of being filtered.
DROP TABLE IF EXISTS t_aj_mismatch;

CREATE TABLE t_aj_mismatch (x UInt64, arr Array(UInt32), arr2 Array(UInt32))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_aj_mismatch VALUES (1, [], [7]), (2, [1, 2], [3, 4]);

SELECT '-- sizes of arrays do not match';
SELECT count() FROM (SELECT x FROM t_aj_mismatch ARRAY JOIN arr, arr2 ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 0; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT count() FROM (SELECT x FROM t_aj_mismatch ARRAY JOIN arr, arr2 ORDER BY x LIMIT 10) SETTINGS query_plan_top_k_through_array_join = 1; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_aj_mismatch;

-- Array(Nullable(T)): NULL elements must survive the guard, which only looks at array length.
DROP TABLE IF EXISTS t_aj_nullable;

CREATE TABLE t_aj_nullable (x UInt64, arr Array(Nullable(UInt32)))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_aj_nullable
SELECT number, if(number % 2 = 0, [NULL, toNullable(toUInt32(number))], [])
FROM numbers(50);

SELECT '-- Array(Nullable)';
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj_nullable ARRAY JOIN arr ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 0;
SELECT x, e FROM (SELECT x, arr AS e FROM t_aj_nullable ARRAY JOIN arr ORDER BY x LIMIT 6) ORDER BY x, e SETTINGS query_plan_top_k_through_array_join = 1;

DROP TABLE t_aj_nullable;

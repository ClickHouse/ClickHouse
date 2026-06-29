SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS tuple_test;

CREATE TABLE tuple_test
(
    id  UInt64,
    tup Nullable(Tuple(u UInt64, s String)),
    n   Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tuple_test (id, tup, n) VALUES
    (1, tuple(11, 'alpha'), 100),
    (2, NULL,                200),
    (3, tuple(33, 'gamma'),  NULL),
    (4, tuple(44, 'delta'),  400),
    (5, tuple(11, 'beta'),   500),
    (6, NULL,                NULL);

SELECT 'Direct column access';
SELECT id, tup, n, isNull(tup), isNull(n)
FROM tuple_test
ORDER BY id;

SELECT 'Element access with dot notation';
SELECT id, tup.u, tup.s, toTypeName(tup.u), toTypeName(tup.s)
FROM tuple_test
ORDER BY id;

SELECT 'Element access with tupleElement';
SELECT id, tupleElement(tup, 'u') as u, tupleElement(tup, 's') as s
FROM tuple_test
ORDER BY id;

SELECT 'Element access by index';
SELECT id, tupleElement(tup, 1) as first, tupleElement(tup, 2) as second
FROM tuple_test
ORDER BY id;

SELECT 'WHERE - IS NULL';
SELECT id, tup FROM tuple_test WHERE tup IS NULL ORDER BY id;

SELECT 'WHERE - IS NOT NULL';
SELECT id, tup FROM tuple_test WHERE tup IS NOT NULL ORDER BY id;

SELECT 'WHERE - Element comparison';
SELECT id, tup.u FROM tuple_test WHERE tup.u > 20 ORDER BY id;

SELECT 'WHERE - Element IS NULL';
SELECT id, tup FROM tuple_test WHERE tup.u IS NULL ORDER BY id;

SELECT 'WHERE - Element IS NOT NULL';
SELECT id, tup FROM tuple_test WHERE tup.u IS NOT NULL ORDER BY id;

SELECT 'WHERE - String element comparison';
SELECT id, tup.s FROM tuple_test WHERE tup.s = 'alpha' ORDER BY id;

SELECT 'WHERE - Multiple conditions';
SELECT id, tup FROM tuple_test
WHERE tup IS NOT NULL AND tup.u > 20 AND tup.s LIKE '%a%'
ORDER BY id;

SELECT 'WHERE - Combined with other nullable column';
SELECT id, tup, n FROM tuple_test
WHERE tup IS NOT NULL OR n IS NOT NULL
ORDER BY id;

SELECT 'ORDER BY - Whole tuple ASC';
SELECT id, tup FROM tuple_test ORDER BY tup, id;

SELECT 'ORDER BY - Whole tuple DESC';
SELECT id, tup FROM tuple_test ORDER BY tup DESC, id;

SELECT 'ORDER BY - Whole tuple NULLS FIRST';
SELECT id, tup FROM tuple_test ORDER BY tup NULLS FIRST, id;

SELECT 'ORDER BY - Whole tuple NULLS LAST';
SELECT id, tup FROM tuple_test ORDER BY tup NULLS LAST, id;

SELECT 'ORDER BY - Element u ASC';
SELECT id, tup.u FROM tuple_test ORDER BY tup.u ASC, id;

SELECT 'ORDER BY - Element u DESC';
SELECT id, tup.u FROM tuple_test ORDER BY tup.u DESC, id;

SELECT 'ORDER BY - Element u NULLS FIRST';
SELECT id, tup.u FROM tuple_test ORDER BY tup.u NULLS FIRST, id;

SELECT 'ORDER BY - Element u NULLS LAST';
SELECT id, tup.u FROM tuple_test ORDER BY tup.u NULLS LAST, id;

SELECT 'ORDER BY - Element s';
SELECT id, tup.s FROM tuple_test ORDER BY tup.s NULLS LAST, id;

SELECT 'ORDER BY - Multiple elements';
SELECT id, tup.u, tup.s FROM tuple_test ORDER BY tup.u NULLS LAST, tup.s, id;

SELECT 'ORDER BY - Mixed with other columns';
SELECT id, tup, n FROM tuple_test ORDER BY n NULLS LAST, tup.u;

SELECT 'GROUP BY - Whole tuple';
SELECT tup, count() as cnt
FROM tuple_test
GROUP BY tup
ORDER BY tup NULLS LAST;

SELECT 'GROUP BY - Element u';
SELECT tup.u, count() as cnt, sum(n) as sum_n
FROM tuple_test
GROUP BY tup.u
ORDER BY tup.u NULLS LAST;

SELECT 'GROUP BY - Element s';
SELECT tup.s, count() as cnt
FROM tuple_test
GROUP BY tup.s
ORDER BY tup.s NULLS LAST;

SELECT 'GROUP BY - Multiple elements';
SELECT tup.u, tup.s, count() as cnt
FROM tuple_test
GROUP BY tup.u, tup.s
ORDER BY tup.u NULLS LAST, tup.s;

SELECT 'GROUP BY - NULL handling';
SELECT
    isNull(tup) as is_null,
    count() as cnt
FROM tuple_test
GROUP BY isNull(tup)
ORDER BY is_null;

SELECT 'GROUP BY - COALESCE with default';
SELECT
    coalesce(tup.u, 0) as u_value,
    count() as cnt
FROM tuple_test
GROUP BY coalesce(tup.u, 0)
ORDER BY u_value;

SELECT 'HAVING - On count';
SELECT tup.u, count() as cnt
FROM tuple_test
GROUP BY tup.u
HAVING cnt > 1
ORDER BY tup.u NULLS LAST;

SELECT 'HAVING - On aggregated value';
SELECT tup.u, sum(n) as sum_n
FROM tuple_test
GROUP BY tup.u
HAVING sum_n > 400
ORDER BY tup.u NULLS LAST;

SELECT 'HAVING - With element condition';
SELECT tup.u, count() as cnt
FROM tuple_test
GROUP BY tup.u
HAVING tup.u > 10
ORDER BY tup.u;

SELECT 'Aggregation - count';
SELECT
    count() as total,
    count(tup) as non_null_tuples,
    count(tup.u) as non_null_u
FROM tuple_test;

SELECT 'Aggregation - min/max on elements';
SELECT
    min(tup.u) as min_u,
    max(tup.u) as max_u,
    min(tup.s) as min_s,
    max(tup.s) as max_s
FROM tuple_test;

SELECT 'Aggregation - sum/avg on elements';
SELECT
    sum(tup.u) as sum_u,
    avg(tup.u) as avg_u
FROM tuple_test;

SELECT 'Aggregation - groupArray';
SELECT
    groupArray(tup) as all_tuples,
    groupArray(tup.u) as all_u_values
FROM tuple_test;

SELECT 'Aggregation - uniq/uniqExact';
SELECT
    uniq(tup) as unique_tuples,
    uniqExact(tup.u) as unique_u_values
FROM tuple_test;

SELECT 'Aggregation - any/anyLast';
SELECT
    any(tup) as any_tuple,
    anyLast(tup.u) as anylast_u
FROM tuple_test;

SELECT 'Aggregation - countIf';
SELECT
    countIf(tup IS NULL) as null_count,
    countIf(tup.u > 20) as u_gt_20_count
FROM tuple_test;

SELECT 'JOIN - Self join on tuple';
SELECT
    t1.id as id1,
    t2.id as id2,
    t1.tup as tup1
FROM tuple_test t1
JOIN tuple_test t2 ON t1.tup = t2.tup
WHERE t1.id < t2.id
ORDER BY id1, id2;

SELECT 'JOIN - On element';
SELECT
    t1.id as id1,
    t2.id as id2,
    t1.tup.u as u_value
FROM tuple_test t1
JOIN tuple_test t2 ON t1.tup.u = t2.tup.u
WHERE t1.id < t2.id
ORDER BY id1, id2
SETTINGS enable_analyzer = 1; -- t1.tup.u notation is not recognized in old analyzer

SELECT 'LEFT JOIN - With NULL handling';
SELECT
    t1.id,
    t1.tup as tup1,
    t2.tup as tup2
FROM tuple_test t1
LEFT JOIN tuple_test t2 ON t1.tup.u = t2.tup.u AND t1.id != t2.id
ORDER BY t1.id, t2.id NULLS LAST
SETTINGS enable_analyzer = 1; -- t1.tup.u notation is not recognized in old analyzer

SELECT 'INNER JOIN - Exclude NULLs';
SELECT
    t1.id as id1,
    t2.id as id2
FROM tuple_test t1
INNER JOIN tuple_test t2 ON t1.tup = t2.tup
WHERE t1.tup IS NOT NULL
ORDER BY id1, id2;

SELECT 'DISTINCT - On tuple';
SELECT DISTINCT tup
FROM tuple_test
ORDER BY tup NULLS LAST;

SELECT 'DISTINCT - On element';
SELECT DISTINCT tup.u
FROM tuple_test
ORDER BY tup.u NULLS LAST;

SELECT 'DISTINCT - Multiple elements';
SELECT DISTINCT tup.u, tup.s
FROM tuple_test
ORDER BY tup.u NULLS LAST, tup.s;

SELECT 'UNION - Tuples';
SELECT tup
FROM
(
    SELECT tup FROM tuple_test WHERE id <= 3
    UNION ALL
    SELECT tup FROM tuple_test WHERE id >= 3
)
ORDER BY tup NULLS LAST;

SELECT 'UNION DISTINCT - Elements';
SELECT value
FROM
(
    SELECT tup.u AS value FROM tuple_test
    UNION DISTINCT
    SELECT n AS value FROM tuple_test
)
ORDER BY value NULLS LAST;

SELECT 'INTERSECT - Elements';
SELECT tup.u FROM tuple_test WHERE tup.u IS NOT NULL
INTERSECT
SELECT n FROM tuple_test WHERE n IS NOT NULL
ORDER BY tup.u;

SELECT 'EXCEPT - Elements';
SELECT tup.u FROM tuple_test WHERE tup.u IS NOT NULL
EXCEPT
SELECT n FROM tuple_test WHERE n IS NOT NULL
ORDER BY tup.u;

SELECT 'Subquery - IN clause';
SELECT id, tup
FROM tuple_test
WHERE tup.u IN (SELECT tup.u FROM tuple_test WHERE tup.s LIKE '%a%')
ORDER BY id;

SELECT 'Subquery - EXISTS';
SELECT id, tup
FROM tuple_test t1
WHERE EXISTS (
    SELECT 1 FROM tuple_test t2
    WHERE t1.tup.u = t2.tup.u AND t1.id != t2.id
)
ORDER BY id
SETTINGS enable_analyzer = 1; -- t1.tup.u notation is not recognized in old analyzer

SELECT 'Subquery - Scalar subquery';
SELECT
    id,
    tup.u,
    (SELECT max(tup.u) FROM tuple_test) as max_u
FROM tuple_test
ORDER BY id;

SELECT 'Subquery - Correlated';
SELECT
    id,
    tup.u,
    (SELECT count() FROM tuple_test t2 WHERE t2.tup.u = t1.tup.u) as count_same_u
FROM tuple_test t1
ORDER BY id
SETTINGS enable_analyzer = 1; -- t1.tup.u notation is not recognized in old analyzer

SELECT 'CASE - On NULL';
SELECT
    id,
    CASE
        WHEN tup IS NULL THEN 'null tuple'
        WHEN tup.u > 30 THEN 'high'
        ELSE 'low'
    END as category
FROM tuple_test
ORDER BY id;

SELECT 'CASE - On element';
SELECT
    id,
    tup.u,
    CASE tup.u
        WHEN 11 THEN 'eleven'
        WHEN 33 THEN 'thirty-three'
        WHEN 44 THEN 'forty-four'
        ELSE 'other'
    END as u_label
FROM tuple_test
ORDER BY id;

SELECT 'IF function';
SELECT
    id,
    if(tup IS NULL, 'NULL', concat('u=', toString(tup.u))) as result
FROM tuple_test
ORDER BY id;

SELECT 'multiIf';
SELECT
    id,
    multiIf(
        tup IS NULL, 'null',
        tup.u < 20, 'small',
        tup.u < 40, 'medium',
        'large'
    ) as size_category
FROM tuple_test
ORDER BY id;

SELECT 'Window - row_number';
SELECT
    id,
    tup.u,
    row_number() OVER (ORDER BY tup.u, id NULLS LAST) as rn
FROM tuple_test
ORDER BY rn, id;

SELECT 'Window - rank/dense_rank';
SELECT
    id,
    tup.u,
    rank() OVER (ORDER BY tup.u NULLS LAST) as rnk,
    dense_rank() OVER (ORDER BY tup.u NULLS LAST) as dense_rnk
FROM tuple_test
ORDER BY id;

SELECT 'Window - PARTITION BY';
SELECT
    id,
    tup.u,
    isNull(tup),
    row_number() OVER (PARTITION BY isNull(tup) ORDER BY tup.u, id NULLS LAST) as rn_in_partition
FROM tuple_test
ORDER BY isNull(tup), rn_in_partition, id;

SELECT 'Window - Aggregate functions';
SELECT
    id,
    tup.u,
    sum(tup.u) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumsum,
    avg(tup.u) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg
FROM tuple_test
ORDER BY id;

SELECT 'Comparison - Tuple equality';
SELECT
    t1.id as id1,
    t2.id as id2,
    t1.tup = t2.tup as is_equal,
    t1.tup != t2.tup as is_not_equal
FROM tuple_test t1, tuple_test t2
WHERE t1.id = 1 AND t2.id IN (1, 2, 3)
ORDER BY id1, id2;

SELECT 'Comparison - Element equality';
SELECT
    id,
    tup.u = 11 as u_is_11,
    tup.s = 'alpha' as s_is_alpha
FROM tuple_test
ORDER BY id;

SELECT 'Comparison - Greater/Less';
SELECT
    id,
    tup.u > 20 as u_gt_20,
    tup.u < 50 as u_lt_50,
    tup.u BETWEEN 10 AND 40 as u_between
FROM tuple_test
ORDER BY id;

SELECT 'Comparison - String operations';
SELECT
    id,
    tup.s LIKE '%a%' as has_a,
    tup.s IN ('alpha', 'beta') as is_alpha_or_beta
FROM tuple_test
ORDER BY id;

SELECT 'Type conversion - toString';
SELECT
    id,
    toString(tup) as tup_str,
    toString(tup.u) as u_str
FROM tuple_test
ORDER BY id;

SELECT 'Type conversion - CAST element';
SELECT
    id,
    CAST(tup.u AS Float64) as u_float,
    CAST(tup.s AS FixedString(10)) as s_fixed
FROM tuple_test
WHERE tup IS NOT NULL
ORDER BY id;

SELECT 'NULL functions - coalesce';
SELECT
    id,
    coalesce(tup.u, 999) as u_with_default,
    coalesce(tup.s, 'N/A') as s_with_default
FROM tuple_test
ORDER BY id;

SELECT 'NULL functions - ifNull';
SELECT
    id,
    ifNull(tup.u, 0) as u_or_zero,
    ifNull(tup.s, 'empty') as s_or_empty
FROM tuple_test
ORDER BY id;

SELECT 'NULL functions - assumeNotNull';
SELECT
    id,
    assumeNotNull(tup.u) as u_not_null,
    toTypeName(assumeNotNull(tup.u)) as type
FROM tuple_test
WHERE tup IS NOT NULL
ORDER BY id;

SELECT 'NULL functions - isNull/isNotNull';
SELECT
    id,
    isNull(tup) as tup_is_null,
    isNotNull(tup) as tup_is_not_null,
    isNull(tup.u) as u_is_null
FROM tuple_test
ORDER BY id;

SELECT 'CTE - Basic';
WITH filtered AS (
    SELECT id, tup
    FROM tuple_test
    WHERE tup IS NOT NULL
)
SELECT id, tup.u, tup.s
FROM filtered
ORDER BY id
SETTINGS enable_analyzer = 1; -- CTE tup.u notation is not recognized in old analyzer

SELECT 'CTE - Aggregation';
WITH stats AS (
    SELECT
        avg(tup.u) as avg_u,
        max(tup.u) as max_u
    FROM tuple_test
)
SELECT
    id,
    tup.u,
    tup.u - (SELECT avg_u FROM stats) as diff_from_avg
FROM tuple_test
WHERE tup IS NOT NULL
ORDER BY id;

SELECT 'Array operations - groupArray';
SELECT groupArray(tup) as tuple_array
FROM tuple_test;

SELECT 'Array operations - arrayElement';
SELECT
    groupArray(tup) as tuple_array,
    tuple_array[1] as first_tuple,
    tuple_array[1].u as first_u
FROM tuple_test;

SELECT 'Array operations - arrayFilter';
SELECT arrayFilter(x -> isNotNull(x), groupArray(tup)) as non_null_tuples
FROM tuple_test;

SELECT 'Array operations - arrayMap';
SELECT arrayMap(x -> x.u, arrayFilter(x -> isNotNull(x), groupArray(tup))) as all_u_values
FROM tuple_test
SETTINGS enable_analyzer = 1; -- Lambda tup.u notation is not recognized in old analyzer

SELECT 'Complex - Multiple operations';
SELECT
    tup.u as u_value,
    count() as cnt,
    sum(n) as total_n,
    avg(n) as avg_n,
    groupArray(id) as ids
FROM tuple_test
WHERE tup IS NOT NULL
GROUP BY tup.u
HAVING cnt > 0 AND avg_n > 200
ORDER BY u_value;

SELECT 'Complex - Nested aggregation';
SELECT
    category,
    count() as cnt,
    avg(tup.u) as avg_u
FROM (
    SELECT
        id,
        tup,
        CASE
            WHEN tup.u < 30 THEN 'low'
            ELSE 'high'
        END as category
    FROM tuple_test
    WHERE tup IS NOT NULL
)
GROUP BY category
ORDER BY category
SETTINGS enable_analyzer = 1; -- Here, tup.u notation is not recognized in old analyzer

DROP TABLE IF EXISTS tuple_test;

SELECT 'Multiple nullable tuple elements';
DROP TABLE IF EXISTS test_nullable_tuple;

CREATE TABLE test_nullable_tuple
(
    data Nullable(Tuple(String, UInt64))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test_nullable_tuple VALUES
    (('Alice', 100)),
    (NULL),
    (('Bob', 200)),
    (NULL),
    (('Charlie', 300));

SELECT
    tupleElement(data, 1), tupleElement(data, 1), tupleElement(data, 2)
FROM test_nullable_tuple;

DROP TABLE IF EXISTS test_nullable_tuple;

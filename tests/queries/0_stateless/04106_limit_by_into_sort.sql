-- { echo }

DROP TABLE IF EXISTS test_mt;
CREATE TABLE test_mt (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_mt SELECT number % 100, number FROM numbers(10000);
INSERT INTO test_mt SELECT number % 100, number FROM numbers(10000);

DROP TABLE IF EXISTS test_join_a;
DROP TABLE IF EXISTS test_join_b;
CREATE TABLE test_join_a (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE test_join_b (k UInt32, w UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_join_a SELECT number % 100, number FROM numbers(1000);
INSERT INTO test_join_a SELECT number % 100, number FROM numbers(1000);
INSERT INTO test_join_b SELECT number % 100, number FROM numbers(1000);
INSERT INTO test_join_b SELECT number % 100, number FROM numbers(1000);

-- `LIMIT BY` keys are not a prefix of the sort keys.
SELECT
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT number % 100 AS k, number AS v
        FROM numbers_mt(1000000)
        ORDER BY v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT number % 100 AS k, number AS v
        FROM numbers_mt(1000000)
        ORDER BY v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- `LIMIT BY` keys are a prefix of the sort keys.
SELECT
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT number % 100 AS k, number AS v
        FROM numbers_mt(1000000)
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT number % 100 AS k, number AS v
        FROM numbers_mt(1000000)
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- The query body inside the JSON `EXPLAIN`.
SELECT
    (SELECT (sum(cityHash64(number)), count()) FROM (
        SELECT number FROM numbers(1000)
        ORDER BY number LIMIT 2, 3 BY number
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(number)), count()) FROM (
        SELECT number FROM numbers(1000)
        ORDER BY number LIMIT 2, 3 BY number
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- Derived `LIMIT BY` expression — not a sort-key prefix.
SELECT
    (SELECT (sum(cityHash64(grp, v)), count()) FROM (
        SELECT number % 100 AS grp, number AS v
        FROM numbers_mt(1000000)
        ORDER BY v LIMIT 2, 3 BY grp % 2
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(grp, v)), count()) FROM (
        SELECT number % 100 AS grp, number AS v
        FROM numbers_mt(1000000)
        ORDER BY v LIMIT 2, 3 BY grp % 2
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- Read-in-order MergeTree input that already satisfies the full sort.
SELECT
    (SELECT (sum(cityHash64(a, b)), count()) FROM (
        SELECT a, b FROM test_mt ORDER BY a, b LIMIT 2, 3 BY a
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(a, b)), count()) FROM (
        SELECT a, b FROM test_mt ORDER BY a, b LIMIT 2, 3 BY a
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- `FinishSorting` with an extra suffix sort key.
SELECT
    (SELECT (sum(cityHash64(a, b)), count()) FROM (
        SELECT a, b FROM test_mt ORDER BY a, b, b + 1 LIMIT 2, 3 BY a
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(a, b)), count()) FROM (
        SELECT a, b FROM test_mt ORDER BY a, b, b + 1 LIMIT 2, 3 BY a
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- `LIMIT BY` with no `ORDER BY` — the optimization does not fire, and the
-- surviving row set is not deterministic without a sort. Compare row counts only.
SELECT
    (SELECT count() FROM (
        SELECT number % 1000 AS k FROM numbers_mt(1000000) LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT count() FROM (
        SELECT number % 1000 AS k FROM numbers_mt(1000000) LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- `LIMIT BY` offset plus length overflows `UInt64` — result is empty.
SELECT
    (SELECT (sum(cityHash64(k)), count()) FROM (
        SELECT number % 1000 AS k FROM numbers_mt(1000000)
        ORDER BY k LIMIT 18446744073709551615, 10 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(k)), count()) FROM (
        SELECT number % 1000 AS k FROM numbers_mt(1000000)
        ORDER BY k LIMIT 18446744073709551615, 10 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- Sort above `UNION ALL`.
SELECT
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT k, v FROM (
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
            UNION ALL
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000))
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT k, v FROM (
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
            UNION ALL
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000))
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- `LIMIT BY` inside one `UNION ALL` branch.
SELECT
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT k, v FROM (
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
            ORDER BY k, v LIMIT 2, 3 BY k
            UNION ALL
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000))
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT k, v FROM (
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
            ORDER BY k, v LIMIT 2, 3 BY k
            UNION ALL
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000))
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- Sort above `JOIN`.
SELECT
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT a.k AS k, a.v AS v
        FROM test_join_a a JOIN test_join_b b ON a.k = b.k
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT a.k AS k, a.v AS v
        FROM test_join_a a JOIN test_join_b b ON a.k = b.k
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1));

-- Sort above `JOIN` with `full_sorting_merge`.
SELECT
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT a.k AS k, a.v AS v
        FROM test_join_a a JOIN test_join_b b ON a.k = b.k
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0, join_algorithm = 'full_sorting_merge'))
    =
    (SELECT (sum(cityHash64(k, v)), count()) FROM (
        SELECT a.k AS k, a.v AS v
        FROM test_join_a a JOIN test_join_b b ON a.k = b.k
        ORDER BY k, v LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1, join_algorithm = 'full_sorting_merge'));

-- `WindowStep` separates `LimitByStep` from `SortingStep` — the optimization does not fire.
SELECT
    (SELECT count() FROM (
        SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn
        FROM (
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
            ORDER BY k, v)
        LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 0))
    =
    (SELECT count() FROM (
        SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn
        FROM (
            SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
            ORDER BY k, v)
        LIMIT 2, 3 BY k
        SETTINGS query_plan_push_limit_by_into_sort = 1));

DROP TABLE test_mt;
DROP TABLE test_join_a;
DROP TABLE test_join_b;

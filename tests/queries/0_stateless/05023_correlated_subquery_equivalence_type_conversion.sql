-- Correlated-subquery equivalent-expression substitution across compatible types:
-- nullability-only differences and native-number pairs with a least supertype.
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/113407

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET correlated_subqueries_substitute_equivalent_expressions = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET correlated_subqueries_default_join_kind = 'left';
SET enable_parallel_replicas = 0;

-- Case 1 tables: the reproducer from the issue.
CREATE TABLE customer (c_customer_sk Int64 NOT NULL, c_current_cdemo_sk Nullable(Int64), c_current_addr_sk Nullable(Int64)) ENGINE=MergeTree ORDER BY c_customer_sk;
CREATE TABLE customer_address (ca_address_sk Int64 NOT NULL) ENGINE=MergeTree ORDER BY ca_address_sk;
CREATE TABLE customer_demographics (cd_demo_sk Int64 NOT NULL) ENGINE=MergeTree ORDER BY cd_demo_sk;
CREATE TABLE store_sales (ss_customer_sk Nullable(Int64), ss_sold_date_sk Nullable(UInt32)) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE date_dim (d_date_sk UInt32 NOT NULL, d_year UInt32, d_qoy UInt32) ENGINE=MergeTree ORDER BY d_date_sk;

INSERT INTO customer VALUES (1,10,100);
INSERT INTO customer_address VALUES (100);
INSERT INTO customer_demographics VALUES (10);
INSERT INTO date_dim VALUES (1,2002,1);
INSERT INTO store_sales VALUES (1,1);

-- Cases 2, 6, 7: outer `Int64`, inner `Nullable(Int64)`.
CREATE TABLE t_outer_2 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_2 (x Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_2 VALUES (0), (1), (2), (3);
INSERT INTO t_inner_2 VALUES (NULL), (1), (NULL), (3), (42);

-- Case 3: outer `Nullable(Int64)`, inner `Int64`.
CREATE TABLE t_outer_3 (x Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_3 (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_3 VALUES (NULL), (0), (1), (2);
INSERT INTO t_inner_3 VALUES (1), (5);

-- Case 4: scalar subquery with aggregation, inner `Nullable(Int64)` with NULLs and a genuine 0.
CREATE TABLE t_outer_4 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_4 (x Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_4 VALUES (0), (1), (2);
INSERT INTO t_inner_4 VALUES (NULL), (NULL), (0), (1), (1);

-- Case 5: correlated column used in a second expression inside the subquery.
CREATE TABLE t_outer_5 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_5 (x Nullable(Int64), y Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_5 VALUES (0), (1), (2), (3);
INSERT INTO t_inner_5 VALUES (NULL, 100), (1, 10), (2, 1), (3, 3);

-- Case 8: outer `Int64`, inner `Int32` and `Nullable(Int32)`.
CREATE TABLE t_outer_8 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_8 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_8n (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_8 VALUES (-5), (3), (7), (2147483648);
INSERT INTO t_inner_8 VALUES (-5), (7), (100);
INSERT INTO t_inner_8n VALUES (NULL), (3), (100);

-- Case 9: outer `Int32`, inner `Int64` with out-of-range values. 705032704 is 5000000000 mod 2^32.
CREATE TABLE t_outer_9 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_9 (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_9 VALUES (1), (2), (705032704);
INSERT INTO t_inner_9 VALUES (1), (5000000000), (-9000000000);

-- Case 10: outer `Int32`, inner `UInt32`. -1294967296 is 3000000000 wrapped to `Int32`.
CREATE TABLE t_outer_10a (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_10a (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_10a VALUES (-1294967296), (5), (42);
INSERT INTO t_inner_10a VALUES (3000000000), (5);

-- Case 10: outer `Int32`, inner `Float64`. Outer 1 and 2 catch a truncating/rounding cast of 1.5.
CREATE TABLE t_outer_10b (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_10b (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_10b VALUES (1), (2), (3);
INSERT INTO t_inner_10b VALUES (1.5), (3.0);

-- Case 11: outer `Float64`, inner `Int32`.
CREATE TABLE t_outer_11 (x Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_11 (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_11 VALUES (1.5), (3), (100.25);
INSERT INTO t_inner_11 VALUES (1), (3), (100);

-- Case 12: guarded fallbacks. `Float64` vs `Int64` around 2^53, `Int64` vs `UInt64` above `Int64` max,
-- `String` vs `FixedString(2)` with padding-sensitive values.
CREATE TABLE t_outer_12a (x Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_12a (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_12a VALUES (9007199254740992), (123);
INSERT INTO t_inner_12a VALUES (9007199254740993), (123);
CREATE TABLE t_outer_12b (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_12b (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_12b VALUES (-1), (42), (9223372036854775807);
INSERT INTO t_inner_12b VALUES (42), (9223372036854775808), (18446744073709551615);
CREATE TABLE t_outer_12c (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_12c (s FixedString(2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_12c VALUES ('a'), ('bb'), ('zz');
INSERT INTO t_inner_12c VALUES ('a'), ('bb');

-- {echoOn}
-- Case 1: issue reproducer, nullability-only mismatch must not plan a CROSS JOIN.
SELECT countIf(explain ILIKE '%cross%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT * FROM customer c, customer_address ca, customer_demographics cd
    WHERE c.c_current_addr_sk = ca.ca_address_sk AND cd.cd_demo_sk = c.c_current_cdemo_sk
    AND EXISTS (
        SELECT * FROM store_sales, date_dim
        WHERE c.c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
          AND d_year = 2002 AND d_qoy < 4
    )
);

-- Case 1: the reproducer must return its single row.
SELECT c_customer_sk, c_current_cdemo_sk, c_current_addr_sk, ca_address_sk, cd_demo_sk
FROM customer c, customer_address ca, customer_demographics cd
WHERE c.c_current_addr_sk = ca.ca_address_sk AND cd.cd_demo_sk = c.c_current_cdemo_sk
AND EXISTS (
    SELECT * FROM store_sales, date_dim
    WHERE c.c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
      AND d_year = 2002 AND d_qoy < 4
)
ORDER BY c_customer_sk;

-- Case 2: EXISTS, inner side `Nullable(Int64)`: inner NULL must not match, in particular not outer 0.
SELECT x FROM t_outer_2 AS o WHERE EXISTS (SELECT 1 FROM t_inner_2 AS i WHERE i.x = o.x) ORDER BY x;

-- Case 3: EXISTS, outer side `Nullable(Int64)`: outer NULL rows must not match.
SELECT x FROM t_outer_3 AS o WHERE EXISTS (SELECT 1 FROM t_inner_3 AS i WHERE i.x = o.x) ORDER BY x;

-- Case 4: scalar subquery with `count`: outer 0 must count only genuine 0 rows, not inner NULLs.
SELECT x, (SELECT count() FROM t_inner_4 AS i WHERE i.x = o.x) FROM t_outer_4 AS o ORDER BY x;

-- Case 5: correlated column also used in a second expression inside the subquery.
SELECT x FROM t_outer_5 AS o WHERE EXISTS (SELECT 1 FROM t_inner_5 AS i WHERE i.x = o.x AND i.y + o.x > 5) ORDER BY x;

-- Case 6: NOT EXISTS complement of case 2.
SELECT x FROM t_outer_2 AS o WHERE NOT EXISTS (SELECT 1 FROM t_inner_2 AS i WHERE i.x = o.x) ORDER BY x;

-- Case 7: minimal two-table plan check for the nullability-only mismatch.
SELECT countIf(explain ILIKE '%cross%') FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_2 AS o WHERE EXISTS (SELECT 1 FROM t_inner_2 AS i WHERE i.x = o.x));

-- Case 8: widening, outer `Int64` vs inner `Int32`: no cross join, exact matches only.
SELECT countIf(explain ILIKE '%cross%') FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_8 AS o WHERE EXISTS (SELECT 1 FROM t_inner_8 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_8 AS o WHERE EXISTS (SELECT 1 FROM t_inner_8 AS i WHERE i.x = o.x) ORDER BY x;

-- Case 8: the `Nullable(Int32)` inner variant.
SELECT countIf(explain ILIKE '%cross%') FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_8 AS o WHERE EXISTS (SELECT 1 FROM t_inner_8n AS i WHERE i.x = o.x));
SELECT x FROM t_outer_8 AS o WHERE EXISTS (SELECT 1 FROM t_inner_8n AS i WHERE i.x = o.x) ORDER BY x;

-- Case 9: narrowing, outer `Int32` vs inner `Int64`: no cross join, out-of-range 5000000000 must not match 705032704.
SELECT countIf(explain ILIKE '%cross%') FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_9 AS o WHERE EXISTS (SELECT 1 FROM t_inner_9 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_9 AS o WHERE EXISTS (SELECT 1 FROM t_inner_9 AS i WHERE i.x = o.x) ORDER BY x;

-- Case 10: sideways supertype, outer `Int32` vs inner `UInt32`: no cross join, 3000000000 must not match -1294967296.
SELECT countIf(explain ILIKE '%cross%') FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_10a AS o WHERE EXISTS (SELECT 1 FROM t_inner_10a AS i WHERE i.x = o.x));
SELECT x FROM t_outer_10a AS o WHERE EXISTS (SELECT 1 FROM t_inner_10a AS i WHERE i.x = o.x) ORDER BY x;

-- Case 10: outer `Int32` vs inner `Float64`: no cross join, 1.5 must not match 1 or 2, 3.0 must match 3.
SELECT countIf(explain ILIKE '%cross%') FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_10b AS o WHERE EXISTS (SELECT 1 FROM t_inner_10b AS i WHERE i.x = o.x));
SELECT x FROM t_outer_10b AS o WHERE EXISTS (SELECT 1 FROM t_inner_10b AS i WHERE i.x = o.x) ORDER BY x;

-- Case 11: lossless float direction, outer `Float64` vs inner `Int32`: no cross join, 1.5 and 100.25 must not match.
SELECT countIf(explain ILIKE '%cross%') FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_11 AS o WHERE EXISTS (SELECT 1 FROM t_inner_11 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_11 AS o WHERE EXISTS (SELECT 1 FROM t_inner_11 AS i WHERE i.x = o.x) ORDER BY x;

-- Case 12: guarded fallback, outer `Float64` vs inner `Int64`: comparison is exact, 9007199254740992.0 must not match 9007199254740993.
SELECT x FROM t_outer_12a AS o WHERE EXISTS (SELECT 1 FROM t_inner_12a AS i WHERE i.x = o.x) ORDER BY x;

-- Case 12: guarded fallback, outer `Int64` vs inner `UInt64`: -1 must not match 18446744073709551615.
SELECT x FROM t_outer_12b AS o WHERE EXISTS (SELECT 1 FROM t_inner_12b AS i WHERE i.x = o.x) ORDER BY x;

-- Case 12: guarded fallback, `String` vs `FixedString(2)`: comparison ignores padding, 'a' must match toFixedString('a', 2).
SELECT s FROM t_outer_12c AS o WHERE EXISTS (SELECT 1 FROM t_inner_12c AS i WHERE i.s = o.s) ORDER BY s;

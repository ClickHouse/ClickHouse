SET enable_analyzer = 1;
SET optimize_extract_common_expressions = 1;

DROP TABLE IF EXISTS x;
CREATE TABLE x (x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO x
    SELECT
        cityHash64(number) AS x,
        cityHash64(number + 1) % 2 AS A,
        cityHash64(number + 2) % 2 AS B,
        cityHash64(number + 3) % 2 AS C,
        cityHash64(number + 4) % 2 AS D,
        cityHash64(number + 5) % 2 AS E,
        cityHash64(number + 6) % 2 AS F
    FROM numbers(2000);

-- Verify that optimization optimization setting works as expected
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE (A AND B) OR (A AND C) SETTINGS optimize_extract_common_expressions = 0;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE (A AND B) OR (A AND C) SETTINGS optimize_extract_common_expressions = 1;

-- Test multiple cases
SELECT * FROM x WHERE A AND ((B AND C) OR (B AND C AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C) OR (B AND C AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE A AND ((B AND C) OR (B AND C AND F));

SELECT * FROM x WHERE A AND ((B AND C AND E) OR (B AND C AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C AND E) OR (B AND C AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE A AND ((B AND C AND E) OR (B AND C AND F));

SELECT * FROM x WHERE A AND ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE A AND ((B AND (C AND E)) OR (B AND C AND F));

SELECT * FROM x WHERE A AND ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE A AND ((B AND C) OR (B AND D) OR (B AND E));

SELECT * FROM x WHERE A AND ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE A AND ((B AND C) OR ((B AND D) OR (B AND E)));

-- Without AND as a root
SELECT * FROM x WHERE ((B AND C) OR (B AND C AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C) OR (B AND C AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((B AND C) OR (B AND C AND F));

SELECT * FROM x WHERE ((B AND C AND E) OR (B AND C AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C AND E) OR (B AND C AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((B AND C AND E) OR (B AND C AND F));

SELECT * FROM x WHERE ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((B AND (C AND E)) OR (B AND C AND F));

SELECT * FROM x WHERE ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((B AND C) OR (B AND D) OR (B AND E));

SELECT * FROM x WHERE ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((B AND C) OR ((B AND D) OR (B AND E)));

-- Complex expression
SELECT * FROM x WHERE (A AND (sipHash64(C) = sipHash64(D))) OR (B AND (sipHash64(C) = sipHash64(D))) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE (A AND (sipHash64(C) = sipHash64(D))) OR (B AND (sipHash64(C) = sipHash64(D))) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE (A AND (sipHash64(C) = sipHash64(D))) OR (B AND (sipHash64(C) = sipHash64(D)));

-- Flattening is only happening if something can be extracted
SELECT * FROM x WHERE ((A AND B) OR ((C AND D) OR (E AND F))) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((A AND B) OR ((C AND D) OR (E AND F))) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((A AND B) OR ((C AND D) OR (E AND F)));

SELECT * FROM x WHERE ((A AND B) OR ((B AND D) OR (E AND F))) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((A AND B) OR ((B AND D) OR (E AND F))) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((A AND B) OR ((B AND D) OR (E AND F)));

-- Duplicates
SELECT * FROM x WHERE (A AND B AND C) OR ((A AND A AND A AND B AND B AND E AND E) OR (A AND B AND B AND F AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE (A AND B AND C) OR ((A AND A AND A AND B AND B AND E AND E) OR (A AND B AND B AND F AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE (A AND B AND C) OR ((A AND A AND A AND B AND B AND E AND E) OR (A AND B AND B AND F AND F));

SELECT * FROM x WHERE ((A AND B AND C) OR (A AND B AND D)) AND ((B AND A AND E) OR (B AND A AND F)) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((A AND B AND C) OR (A AND B AND D)) AND ((B AND A AND E) OR (B AND A AND F)) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((A AND B AND C) OR (A AND B AND D)) AND ((B AND A AND E) OR (B AND A AND F));


-- _CAST function has to be used to maintain the same result type
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((B AND C) OR (B AND C AND toNullable(F)));
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE (x AND x) OR (x AND x);
-- Here the result type stays nullable because of `toNullable(C)`, so no cast is needed
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE ((B AND toNullable(C)) OR (B AND toNullable(C) AND toNullable(F)));

-- Check that optimization only happen on top level, (C AND D) OR (C AND E) shouldn't be optimized
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x WHERE A OR (B AND ((C AND D) OR (C AND E)));


DROP TABLE IF EXISTS y;
CREATE TABLE y (x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO y
    SELECT
        murmurHash3_64(number) AS x,
        murmurHash3_64(number + 1) % 2 AS A,
        murmurHash3_64(number + 2) % 2 AS B,
        murmurHash3_64(number + 3) % 2 AS C,
        murmurHash3_64(number + 4) % 2 AS D,
        murmurHash3_64(number + 5) % 2 AS E,
        murmurHash3_64(number + 6) % 2 AS F
    FROM numbers(2000);

-- JOIN expressions
-- As the optimization code is shared between ON and WHERE, it is enough to test that the optimization is done also in ON
SELECT * FROM x INNER JOIN y ON ((x.A = y.A ) AND x.B = 1) OR ((x.A = y.A) AND y.C = 1) ORDER BY ALL LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x INNER JOIN y ON ((x.A = y.A ) AND x.B = 1) OR ((x.A = y.A) AND y.C = 1) ORDER BY ALL LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x INNER JOIN y ON ((x.A = y.A ) AND x.B = 1) OR ((x.A = y.A) AND y.C = 1);

-- Check that optimization only happen on top level, (x.C = y.C AND x.D = y.D) OR (x.C = y.C AND x.E = y.E) shouldn't be optimized
EXPLAIN QUERY TREE dump_ast = 1 SELECT count() FROM x INNER JOIN y ON (x.A = y.A) OR ((x.B = y.B) AND ((x.C = y.C AND x.D = y.D) OR (x.C = y.C AND x.E = y.E)));

-- Duplicated subexpressions, found by fuzzer
SELECT * FROM x WHERE (D AND 5) OR ((C AND E) AND (C AND E)) ORDER BY ALL LIMIT 3 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE (D AND 5) OR ((C AND E) AND (C AND E)) ORDER BY ALL LIMIT 3;
EXPLAIN QUERY TREE dump_ast = 1 SELECT * FROM x WHERE (C AND E) OR ((C AND E) AND (C AND E));

-- HAVING
SELECT x, max(A) AS mA, max(B) AS mB, max(C) AS mC FROM x GROUP BY x HAVING (mA AND mB) OR (mA AND mC) ORDER BY x LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT x, max(A) AS mA, max(B) AS mB, max(C) AS mC FROM x GROUP BY x HAVING (mA AND mB) OR (mA AND mC) ORDER BY x LIMIT 10;
EXPLAIN QUERY TREE dump_ast = 1 SELECT x, max(A) AS mA, max(B) AS mB, max(C) AS mC FROM x GROUP BY x HAVING (mA AND mB) OR (mA AND mC);

-- QUALIFY
SELECT
    x,
    max(A) OVER (PARTITION BY x % 1000) AS mA,
    max(B) OVER (PARTITION BY x % 1000) AS mB,
    max(C) OVER (PARTITION BY x % 1000) AS mC
FROM x
QUALIFY (mA AND mB) OR (mA AND mC)
ORDER BY x
LIMIT 10
SETTINGS optimize_extract_common_expressions = 0;

SELECT
    x,
    max(A) OVER (PARTITION BY x % 1000) AS mA,
    max(B) OVER (PARTITION BY x % 1000) AS mB,
    max(C) OVER (PARTITION BY x % 1000) AS mC
FROM x
QUALIFY (mA AND mB) OR (mA AND mC)
ORDER BY x
LIMIT 10;

EXPLAIN QUERY TREE dump_ast = 1
SELECT
    x,
    max(A) OVER (PARTITION BY x % 1000) AS mA,
    max(B) OVER (PARTITION BY x % 1000) AS mB,
    max(C) OVER (PARTITION BY x % 1000) AS mC
FROM x
QUALIFY (mA AND mB) OR (mA AND mC);

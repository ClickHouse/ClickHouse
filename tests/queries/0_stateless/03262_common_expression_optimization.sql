SET enable_analyzer = 1;

DROP TABLE IF EXISTS x;
CREATE TABLE x (x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO x SELECT x, A%2 AS A, B%2 AS B, C%2 AS C, D%2 AS D, E%2 AS E, F%2 AS F FROM generateRandom('x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8', 42) LIMIT 2000;

-- Verify that optimization optimization setting works as expected
EXPLAIN QUERY TREE SELECT count() FROM x WHERE (A AND B) OR (A AND C) SETTINGS optimize_extract_common_expressions = 0;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE (A AND B) OR (A AND C) SETTINGS optimize_extract_common_expressions = 1;

-- Test multiple cases
SELECT * FROM x WHERE A AND ((B AND C) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE A AND ((B AND C) OR (B AND C AND F));

SELECT * FROM x WHERE A AND ((B AND C AND E) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C AND E) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE A AND ((B AND C AND E) OR (B AND C AND F));

SELECT * FROM x WHERE A AND ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE A AND ((B AND (C AND E)) OR (B AND C AND F));

SELECT * FROM x WHERE A AND ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE A AND ((B AND C) OR (B AND D) OR (B AND E));

SELECT * FROM x WHERE A AND ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE A AND ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE A AND ((B AND C) OR ((B AND D) OR (B AND E)));

-- Without AND as a root
SELECT * FROM x WHERE ((B AND C) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE ((B AND C) OR (B AND C AND F));

SELECT * FROM x WHERE ((B AND C AND E) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C AND E) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE ((B AND C AND E) OR (B AND C AND F));

SELECT * FROM x WHERE ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND (C AND E)) OR (B AND C AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE ((B AND (C AND E)) OR (B AND C AND F));

SELECT * FROM x WHERE ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C) OR (B AND D) OR (B AND E)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE ((B AND C) OR (B AND D) OR (B AND E));

SELECT * FROM x WHERE ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((B AND C) OR ((B AND D) OR (B AND E))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE ((B AND C) OR ((B AND D) OR (B AND E)));

-- Complex expression
SELECT * FROM x WHERE (A AND (sipHash64(C) = sipHash64(D))) OR (B AND (sipHash64(C) = sipHash64(D))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE (A AND (sipHash64(C) = sipHash64(D))) OR (B AND (sipHash64(C) = sipHash64(D))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE (A AND (sipHash64(C) = sipHash64(D))) OR (B AND (sipHash64(C) = sipHash64(D)));

-- Flattening is only happening if something can be extracted
SELECT * FROM x WHERE ((A AND B) OR ((C AND D) OR (E AND F))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((A AND B) OR ((C AND D) OR (E AND F))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE ((A AND B) OR ((C AND D) OR (E AND F)));

SELECT * FROM x WHERE ((A AND B) OR ((B AND D) OR (E AND F))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE ((A AND B) OR ((B AND D) OR (E AND F))) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE ((A AND B) OR ((B AND D) OR (E AND F)));

-- Duplicates
SELECT * FROM x WHERE (A AND B AND C) OR ((A AND A AND A AND B AND B AND E AND E) OR (A AND B AND B AND F AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 0;
SELECT * FROM x WHERE (A AND B AND C) OR ((A AND A AND A AND B AND B AND E AND E) OR (A AND B AND B AND F AND F)) ORDER BY A, B, C, D, E, F LIMIT 10 SETTINGS optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x WHERE (A AND B AND C) OR ((A AND A AND A AND B AND B AND E AND E) OR (A AND B AND B AND F AND F));

DROP TABLE IF EXISTS y;
CREATE TABLE y (x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO y SELECT x, A%2 AS A, B%2 AS B, C%2 AS C, D%2 AS D, E%2 AS E, F%2 AS F FROM generateRandom('x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8', 43) LIMIT 2000;

-- JOIN expressions
-- As the optimization code is shared between ON and WHERE, it is enough to test that the optimization is done also in ON
SELECT * FROM x INNER JOIN y ON ((x.A = y.A ) AND x.B = 1) OR ((x.A = y.A) AND y.C = 1) ORDER BY ALL LIMIT 10 SETTINGS allow_experimental_join_condition = 1, optimize_extract_common_expressions = 0;
SELECT * FROM x INNER JOIN y ON ((x.A = y.A ) AND x.B = 1) OR ((x.A = y.A) AND y.C = 1) ORDER BY ALL LIMIT 10 SETTINGS allow_experimental_join_condition = 1, optimize_extract_common_expressions = 1;
EXPLAIN QUERY TREE SELECT count() FROM x INNER JOIN y ON ((x.A = y.A ) AND x.B = 1) OR ((x.A = y.A) AND y.C = 1);

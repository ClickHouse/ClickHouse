-- Tags: no-random-settings

DROP TABLE IF EXISTS x;
CREATE TABLE x (x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO x SELECT * FROM generateRandom('x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8') LIMIT 10000;

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE A AND ((B AND C) OR (B AND C AND F));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE A AND ((B AND C AND E) OR (B AND C AND F));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE A AND ((B AND (C AND E)) OR (B AND C AND F));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE A AND ((B AND C) OR (B AND D) OR (B AND E));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE A AND ((B AND C) OR ((B AND D) OR (B AND E)));

-- Without AND as a root
EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE ((B AND C) OR (B AND C AND F));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE ((B AND C AND E) OR (B AND C AND F));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE ((B AND (C AND E)) OR (B AND C AND F));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE ((B AND C) OR (B AND D) OR (B AND E));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE ((B AND C) OR ((B AND D) OR (B AND E)));

-- Complex expression
EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE (A AND (sipHash64(C) = sipHash64(D))) OR (B AND (sipHash64(C) = sipHash64(D)));

-- Flattening is only happening if something can be extracted
EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE ((A AND B) OR ((C AND D) OR (E AND F)));

EXPLAIN QUERY TREE SELECT count()
FROM x
WHERE ((A AND B) OR ((B AND D) OR (E AND F)));

-- Duplicates
EXPLAIN QUERY TREE
SELECT count()
FROM x
WHERE (A AND B AND C) OR ((A AND A AND A AND B AND B AND E AND E) OR (A AND B AND B AND F AND F));

CREATE TABLE y (x Int64, A UInt8, B UInt8, C UInt8, D UInt8, E UInt8, F UInt8) ENGINE = MergeTree ORDER BY x;

-- JOIN expressions
-- As the optimization code is shared between ON and WHERE, it is enough to test that the optimization is done also in ON
EXPLAIN QUERY TREE
SELECT count()
FROM x INNER JOIN y ON ((x.A = y.A ) AND x.B > 1) OR ((x.A = y.A) AND y.B > 1);

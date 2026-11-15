-- `JOIN ... USING` widens the key to a common supertype before the join is planned. The
-- lookup-index join (`table_join`) probes a hash map built from the stored key column, so
-- a probe column whose type was widened to `Nullable` / `LowCardinality` would hash
-- differently from identical stored keys and miss matches. The fast path must therefore
-- either fall back to the regular join or probe with a column of exactly the stored type.
-- These queries return wrong (empty or partial) results if that guarantee regresses.

SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS t_lookup_using_dim SYNC;
DROP TABLE IF EXISTS t_lookup_using_fact SYNC;
DROP TABLE IF EXISTS t_lookup_using_fact_nullable SYNC;
DROP TABLE IF EXISTS t_lookup_using_fact_lc SYNC;

CREATE TABLE t_lookup_using_dim
(
    id UInt64,
    val String,
    LOOKUP INDEX idx_join (id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_lookup_using_fact (id UInt64, payload String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_lookup_using_fact_nullable (id Nullable(UInt64), payload String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_lookup_using_fact_lc (id LowCardinality(UInt64), payload String)
ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_suspicious_low_cardinality_types = 1;

INSERT INTO t_lookup_using_dim VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_lookup_using_fact VALUES (1, 'x'), (2, 'y'), (4, 'w');
INSERT INTO t_lookup_using_fact_nullable VALUES (1, 'x'), (2, 'y'), (4, 'w'), (NULL, 'n');
INSERT INTO t_lookup_using_fact_lc VALUES (1, 'x'), (2, 'y'), (4, 'w');

-- Sanity: the exact-type `USING` join is the case the fast path serves.
SELECT 'using: exact key type';
SELECT f.id, d.val FROM t_lookup_using_fact f JOIN t_lookup_using_dim d USING (id) ORDER BY f.id;

-- Left key Nullable(UInt64) vs stored UInt64: the `USING` supertype is Nullable(UInt64).
SELECT 'using: nullable left key, inner';
SELECT f.id, d.val FROM t_lookup_using_fact_nullable f JOIN t_lookup_using_dim d USING (id) ORDER BY f.id;

SELECT 'using: nullable left key, left';
SELECT f.id, d.val FROM t_lookup_using_fact_nullable f LEFT JOIN t_lookup_using_dim d USING (id) ORDER BY f.id;

SELECT 'using: nullable left key, left, join_use_nulls';
SELECT f.id, d.val FROM t_lookup_using_fact_nullable f LEFT JOIN t_lookup_using_dim d USING (id) ORDER BY f.id
SETTINGS join_use_nulls = 1;

SELECT 'using: nullable left key, any';
SELECT f.id, d.val FROM t_lookup_using_fact_nullable f ANY LEFT JOIN t_lookup_using_dim d USING (id) ORDER BY f.id;

-- Left key LowCardinality(UInt64) vs stored UInt64.
SELECT 'using: low cardinality left key, inner';
SELECT f.id, d.val FROM t_lookup_using_fact_lc f JOIN t_lookup_using_dim d USING (id) ORDER BY f.id;

SELECT 'using: low cardinality left key, left';
SELECT f.id, d.val FROM t_lookup_using_fact_lc f LEFT JOIN t_lookup_using_dim d USING (id) ORDER BY f.id;

-- Three-table `USING`: the widened key reaches the lookup-index table through an
-- intermediate join (the probe is `firstNonDefault` of the earlier sides).
SELECT 'using: three tables, nullable middle';
SELECT a.id, d.val
FROM t_lookup_using_fact a
JOIN t_lookup_using_fact_nullable b USING (id)
JOIN t_lookup_using_dim d USING (id)
ORDER BY a.id;

SELECT 'using: three tables, nullable first';
SELECT b.id, d.val
FROM t_lookup_using_fact_nullable b
JOIN t_lookup_using_fact a USING (id)
JOIN t_lookup_using_dim d USING (id)
ORDER BY b.id;

-- The same widened keys with `direct` as the only allowed algorithm: no hash-join fallback
-- is available, so the direct join must still produce the correct matches.
SELECT 'using: nullable left key, direct only';
SELECT f.id, d.val FROM t_lookup_using_fact_nullable f JOIN t_lookup_using_dim d USING (id) ORDER BY f.id
SETTINGS join_algorithm = 'direct';

SELECT 'using: low cardinality left key, direct only';
SELECT f.id, d.val FROM t_lookup_using_fact_lc f JOIN t_lookup_using_dim d USING (id) ORDER BY f.id
SETTINGS join_algorithm = 'direct';

DROP TABLE t_lookup_using_fact_lc SYNC;
DROP TABLE t_lookup_using_fact_nullable SYNC;
DROP TABLE t_lookup_using_fact SYNC;
DROP TABLE t_lookup_using_dim SYNC;

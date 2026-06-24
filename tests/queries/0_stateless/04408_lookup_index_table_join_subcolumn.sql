-- A `table_join` LOOKUP INDEX builds a direct-join entity from top-level PHYSICAL columns only
-- (`getAllPhysicalColumnsForLookupJoin`). When the right table expression reads a subcolumn (e.g. a
-- `Map` subcolumn) or a virtual column (e.g. `_part`), the lookup entity cannot serve it and the fast
-- path must fall back to the regular join (which materializes the column), instead of raising
-- `Cannot find column ... in table lookup cache`.

SET enable_analyzer = 1;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET max_parallel_replicas = 1;
SET allow_experimental_lookup_index = 1;
SET join_algorithm = 'direct,hash';

DROP TABLE IF EXISTS t_lookup_subcol_dim SYNC;
DROP TABLE IF EXISTS t_lookup_subcol_fact SYNC;

CREATE TABLE t_lookup_subcol_dim
(
    id UInt64,
    val String,
    m Map(String, String),
    LOOKUP INDEX idx_join (id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_lookup_subcol_fact
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_lookup_subcol_dim VALUES (1, 'a', map('k1', 'v1')), (2, 'b', map('k2', 'v2', 'k3', 'v3'));
INSERT INTO t_lookup_subcol_fact VALUES (1, 'x'), (2, 'y');

-- Reading a right-side Map subcolumn must fall back and compute it, not throw.
SELECT 'map subcolumn selected';
SELECT f.id, d.m.keys
FROM t_lookup_subcol_fact AS f
INNER ALL JOIN t_lookup_subcol_dim AS d USING (id)
ORDER BY f.id;

SELECT 'map subcolumn: not using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.m.keys
    FROM t_lookup_subcol_fact AS f
    INNER ALL JOIN t_lookup_subcol_dim AS d USING (id)
);

-- Reading a right-side virtual column (_part) must also fall back.
SELECT 'virtual column selected';
SELECT f.id, isNotNull(d._part) AS has_part
FROM t_lookup_subcol_fact AS f
INNER ALL JOIN t_lookup_subcol_dim AS d USING (id)
ORDER BY f.id;

SELECT 'virtual column: not using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d._part
    FROM t_lookup_subcol_fact AS f
    INNER ALL JOIN t_lookup_subcol_dim AS d USING (id)
);

-- Reading the full physical Map column still takes the lookup fast path.
SELECT 'physical map column only';
SELECT f.id, d.m
FROM t_lookup_subcol_fact AS f
INNER ALL JOIN t_lookup_subcol_dim AS d USING (id)
ORDER BY f.id;

SELECT 'physical map column only: using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.m
    FROM t_lookup_subcol_fact AS f
    INNER ALL JOIN t_lookup_subcol_dim AS d USING (id)
);

DROP TABLE t_lookup_subcol_fact SYNC;
DROP TABLE t_lookup_subcol_dim SYNC;

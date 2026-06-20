-- A `table_join` LOOKUP INDEX builds a direct-join entity from PHYSICAL columns only. When the
-- right table expression reads an `ALIAS` column, the lookup fast path cannot serve it and must
-- fall back to the regular join (which computes the alias), instead of raising
-- `Cannot find column ... in table lookup cache`.

SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;
SET join_algorithm = 'direct,hash';

DROP TABLE IF EXISTS t_lookup_alias_dim SYNC;
DROP TABLE IF EXISTS t_lookup_alias_fact SYNC;

CREATE TABLE t_lookup_alias_dim
(
    id UInt64,
    val String,
    val_alias String ALIAS concat(val, '!'),
    LOOKUP INDEX idx_join (id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_lookup_alias_fact
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_lookup_alias_dim VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_lookup_alias_fact VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- Reading the right-side ALIAS column must compute concat(val, '!') (fall back), not throw.
SELECT 'alias column selected';
SELECT f.id, d.val_alias
FROM t_lookup_alias_fact AS f
INNER ALL JOIN t_lookup_alias_dim AS d USING (id)
ORDER BY f.id;

-- With the alias selected, the plan must fall back (no DirectKeyValueJoin).
SELECT 'alias column: not using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.val_alias
    FROM t_lookup_alias_fact AS f
    INNER ALL JOIN t_lookup_alias_dim AS d USING (id)
);

-- Mixing physical and alias columns also falls back and stays correct.
SELECT 'physical + alias columns selected';
SELECT f.id, d.val, d.val_alias
FROM t_lookup_alias_fact AS f
INNER ALL JOIN t_lookup_alias_dim AS d USING (id)
ORDER BY f.id;

-- Reading only physical right columns still takes the lookup fast path.
SELECT 'physical column only';
SELECT f.id, d.val
FROM t_lookup_alias_fact AS f
INNER ALL JOIN t_lookup_alias_dim AS d USING (id)
ORDER BY f.id;

SELECT 'physical column only: using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.val
    FROM t_lookup_alias_fact AS f
    INNER ALL JOIN t_lookup_alias_dim AS d USING (id)
);

DROP TABLE t_lookup_alias_fact SYNC;
DROP TABLE t_lookup_alias_dim SYNC;

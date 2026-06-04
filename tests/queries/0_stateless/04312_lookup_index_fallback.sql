-- The lookup-index fast paths (`table_set` / `table_join`) must fall back to the regular
-- plan when features they cannot honor are in play, otherwise they would return wrong
-- results or ignore resource limits.

SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS t_lookup_fb_dim SYNC;
DROP TABLE IF EXISTS t_lookup_fb_fact SYNC;

CREATE TABLE t_lookup_fb_dim
(
    id UInt64,
    val String,
    LOOKUP INDEX idx_set (id) TYPE table_set,
    LOOKUP INDEX idx_join (id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_lookup_fb_fact
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_lookup_fb_dim VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_lookup_fb_fact VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- additional_table_filters: the regular plan filters the right table, the lookup fast
-- path must do the same (fall back), so only id = 1 survives.

SELECT 'set: additional_table_filters id = 1';
SELECT id FROM t_lookup_fb_fact WHERE id IN (SELECT id FROM t_lookup_fb_dim) ORDER BY id
SETTINGS additional_table_filters = {'t_lookup_fb_dim':'id = 1'};

-- Use the default join algorithm (which enables the eager lookup-join fast path): with the
-- guard the query must fall back to a regular join that honors the filter.
SELECT 'join: additional_table_filters id = 1';
SELECT f.id, d.val FROM t_lookup_fb_fact f JOIN t_lookup_fb_dim d USING (id)
ORDER BY f.id
SETTINGS additional_table_filters = {'t_lookup_fb_dim':'id = 1'};

-- max_rows_in_set: the lookup `Set` is built with empty SizeLimits; with the fast path
-- disabled, the regular subquery set must enforce the limit and throw.

SELECT 'set: max_rows_in_set = 1';
SELECT id FROM t_lookup_fb_fact WHERE id IN (SELECT id FROM t_lookup_fb_dim) ORDER BY id
SETTINGS max_rows_in_set = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Lookup-join with a Nullable left key: the stored key is non-nullable UInt64, the probe
-- key is Nullable(UInt64). The direct hash entity would hash these differently and miss
-- matches; the fast path must decline and fall back to a correct join.

SELECT 'join: nullable left key';
SELECT f.id, d.val
FROM (SELECT CAST(id AS Nullable(UInt64)) AS id FROM t_lookup_fb_fact) f
JOIN t_lookup_fb_dim d ON f.id = d.id
ORDER BY f.id;

-- Lookup-join with a narrower left key type (UInt32 vs stored UInt64): the types differ
-- (so the entity would hash them differently) but share a common supertype, so the regular
-- join can run after the fast path declines.

SELECT 'join: mismatched left key type';
SELECT f.id, d.val
FROM (SELECT CAST(id AS UInt32) AS id FROM t_lookup_fb_fact) f
JOIN t_lookup_fb_dim d ON f.id = d.id
ORDER BY f.id;

DROP TABLE t_lookup_fb_fact SYNC;
DROP TABLE t_lookup_fb_dim SYNC;

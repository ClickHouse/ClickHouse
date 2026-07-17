-- Tags: no-old-analyzer

-- Keys whose SQL comparison diverges from the compareAt total order the IEJoin operator
-- matches by (Tuple decomposes elementwise with IEEE NaN / NULL propagation, Dynamic unwraps
-- the underlying values) must not become IEJoin conditions. INNER falls back to a cross join
-- with a filter and keeps SQL semantics; as a residual conjunct beyond two scalar inequalities
-- the same comparison is evaluated as a real SQL predicate and stays correct.

SET join_algorithm = 'ie_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS ttk_l;
DROP TABLE IF EXISTS ttk_r;

CREATE TABLE ttk_l (id Int32, tf Tuple(Float64), tn Tuple(Nullable(Int32)), f Float64, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ttk_r (id Int32, tf Tuple(Float64), tn Tuple(Nullable(Int32)), f Float64, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO ttk_l VALUES (1, tuple(nan), tuple(NULL), nan, 1, 10), (2, tuple(2.0), tuple(2), 2.0, 2, 20), (3, tuple(0.5), tuple(3), 0.5, 3, 30);
INSERT INTO ttk_r VALUES (1, tuple(1.0), tuple(1), 1.0, 0, 15), (2, tuple(3.0), tuple(5), 3.0, 1, 25);

-- Tuple keys: not routed (compareAt would order (nan) above (1.0), SQL says the comparison is false).
SELECT 'tuple routed', count() FROM (EXPLAIN SELECT count() FROM ttk_l l JOIN ttk_r r ON l.tf > r.tf AND l.y < r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'tuple inner result', (SELECT arraySort(groupArray((l.id, r.id))) FROM ttk_l l JOIN ttk_r r ON l.tf > r.tf AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ttk_l l, ttk_r r WHERE l.tf > r.tf AND l.y < r.y) AS ok;

-- Tuple with a Nullable element: (NULL) > (1) is NULL, the pair must not match.
SELECT 'nullable tuple routed', count() FROM (EXPLAIN SELECT count() FROM ttk_l l JOIN ttk_r r ON l.tn > r.tn AND l.y < r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'nullable tuple result', (SELECT arraySort(groupArray((l.id, r.id))) FROM ttk_l l JOIN ttk_r r ON l.tn > r.tn AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM ttk_l l, ttk_r r WHERE l.tn > r.tn AND l.y < r.y) AS ok;

-- The outer/anti fallback cannot determine join keys, exactly as without `ie_join` in the list.
SELECT count() FROM ttk_l l LEFT ANTI JOIN ttk_r r ON l.tn > r.tn AND l.y < r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Dynamic keys: not routed even with the setting that admits them as hash join keys.
SET allow_dynamic_type_in_join_keys = 1;
SELECT 'dynamic routed', count() FROM (EXPLAIN SELECT count() FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l JOIN (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r ON l.d > r.d AND l.y < r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'dynamic result', (SELECT arraySort(groupArray((l.id, r.id))) FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l JOIN (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r ON l.d > r.d AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l, (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r WHERE l.d > r.d AND l.y < r.y) AS ok;

-- A tuple comparison beyond two scalar inequalities is a residual condition inside the
-- operator, evaluated with SQL semantics: the NaN-keyed left row matches nothing.
SELECT 'tuple residual routed', count() > 0 FROM (EXPLAIN SELECT count() FROM ttk_l l LEFT SEMI JOIN ttk_r r ON l.x < r.y AND l.y > r.x AND l.tf > r.tf) WHERE explain LIKE '%IEJoin%';
SELECT 'tuple residual semi', (SELECT arraySort(groupArray(l.id)) FROM ttk_l l LEFT SEMI JOIN ttk_r r ON l.x < r.y AND l.y > r.x AND l.tf > r.tf) = (SELECT arraySort(groupArray(id)) FROM (SELECT DISTINCT l.id FROM ttk_l l, ttk_r r WHERE l.x < r.y AND l.y > r.x AND l.tf > r.tf)) AS ok;

DROP TABLE ttk_l;
DROP TABLE ttk_r;

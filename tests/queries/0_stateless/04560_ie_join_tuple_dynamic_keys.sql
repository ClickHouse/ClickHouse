-- Tags: no-old-analyzer

-- Keys whose SQL comparison diverges from the compareAt total order the IEJoin operator
-- matches by (Tuple, Dynamic, Variant) must not become IEJoin conditions. INNER falls back
-- to a cross join with a filter and keeps SQL semantics; as a residual conjunct beyond two
-- scalar inequalities the same comparison is evaluated as a real SQL predicate and stays
-- correct.

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
SELECT 'dynamic routed (keys allowed)', count() FROM (EXPLAIN SELECT count() FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l JOIN (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r ON l.d > r.d AND l.y < r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'dynamic result (keys allowed)', (SELECT arraySort(groupArray((l.id, r.id))) FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l JOIN (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r ON l.d > r.d AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l, (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r WHERE l.d > r.d AND l.y < r.y) AS ok;

-- Without allow_dynamic_type_in_join_keys: still not routed, INNER falls back to the cross
-- join with a filter, the outer kinds keep the pre-existing error.
SET allow_dynamic_type_in_join_keys = 0;
SELECT 'dynamic routed (keys disallowed)', count() FROM (EXPLAIN SELECT count() FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l JOIN (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r ON l.d < r.d AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'dynamic inner (keys disallowed)', count() FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l JOIN (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r ON l.d < r.d AND l.y > r.y;
SELECT count() FROM (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_l) l LEFT JOIN (SELECT id, CAST(f, 'Dynamic') AS d, y FROM ttk_r) r ON l.d < r.d AND l.y > r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A tuple comparison beyond two scalar inequalities is a residual condition inside the
-- operator, evaluated with SQL semantics: the NaN-keyed left row matches nothing.
SELECT 'tuple residual routed', count() > 0 FROM (EXPLAIN SELECT count() FROM ttk_l l LEFT SEMI JOIN ttk_r r ON l.x < r.y AND l.y > r.x AND l.tf > r.tf) WHERE explain LIKE '%IEJoin%';
SELECT 'tuple residual semi', (SELECT arraySort(groupArray(l.id)) FROM ttk_l l LEFT SEMI JOIN ttk_r r ON l.x < r.y AND l.y > r.x AND l.tf > r.tf) = (SELECT arraySort(groupArray(id)) FROM (SELECT DISTINCT l.id FROM ttk_l l, ttk_r r WHERE l.x < r.y AND l.y > r.x AND l.tf > r.tf)) AS ok;

DROP TABLE ttk_l;
DROP TABLE ttk_r;

-- Variant keys: not routed (compareAt would order NULL above 3 and 'abc' above 3, SQL says both comparisons are NULL).
SET variant_throw_on_type_mismatch = 0;

DROP TABLE IF EXISTS tvk_l;
DROP TABLE IF EXISTS tvk_r;

CREATE TABLE tvk_l (id Int32, v Variant(Int32, String), x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tvk_r (id Int32, v Variant(Int32, String), x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tvk_l VALUES (1, 5, 1, 10), (2, 'abc', 2, 20), (3, NULL, 3, 10);
INSERT INTO tvk_r VALUES (1, 3, 0, 15), (2, 'zzz', 1, 25);

SELECT 'variant routed', count() FROM (EXPLAIN SELECT count() FROM tvk_l l JOIN tvk_r r ON l.v > r.v AND l.y < r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'variant inner result', (SELECT arraySort(groupArray((l.id, r.id))) FROM tvk_l l JOIN tvk_r r ON l.v > r.v AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM tvk_l l, tvk_r r WHERE l.v > r.v AND l.y < r.y) AS ok;

-- The outer/anti fallback cannot determine join keys, exactly as without `ie_join` in the list.
SELECT count() FROM tvk_l l LEFT ANTI JOIN tvk_r r ON l.v > r.v AND l.y < r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A variant comparison beyond two scalar inequalities is a residual condition inside the
-- operator, evaluated with SQL semantics: the NULL-keyed left row matches nothing.
SELECT 'variant residual routed', count() > 0 FROM (EXPLAIN SELECT count() FROM tvk_l l LEFT SEMI JOIN tvk_r r ON l.x < r.y AND l.y > r.x AND l.v > r.v) WHERE explain LIKE '%IEJoin%';
SELECT 'variant residual semi', (SELECT arraySort(groupArray(l.id)) FROM tvk_l l LEFT SEMI JOIN tvk_r r ON l.x < r.y AND l.y > r.x AND l.v > r.v) = (SELECT arraySort(groupArray(id)) FROM (SELECT DISTINCT l.id FROM tvk_l l, tvk_r r WHERE l.x < r.y AND l.y > r.x AND l.v > r.v)) AS ok;

-- With the default variant_throw_on_type_mismatch the fallback filter keeps the SQL behavior:
-- comparing mismatched alternatives is an error, not a silent discriminator order.
SET variant_throw_on_type_mismatch = 1;
SELECT count() FROM tvk_l l JOIN tvk_r r ON l.v > r.v AND l.y < r.y; -- { serverError NO_COMMON_TYPE }

DROP TABLE tvk_l;
DROP TABLE tvk_r;

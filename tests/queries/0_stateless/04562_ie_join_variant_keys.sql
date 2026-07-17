-- Tags: no-old-analyzer

-- Keys whose SQL comparison diverges from the compareAt total order the IEJoin operator
-- matches by must not become IEJoin conditions: comparison of Variant unwraps the underlying
-- values, yields NULL for NULL values and mismatched alternatives (or throws, depending on
-- variant_throw_on_type_mismatch), while compareAt orders NULL by hint and alternatives by
-- discriminator. INNER falls back to a cross join with a filter and keeps SQL semantics;
-- as a residual conjunct beyond two scalar inequalities the same comparison is evaluated
-- as a real SQL predicate and stays correct.

SET join_algorithm = 'ie_join,hash';
SET cross_to_inner_join_rewrite = 0;
SET variant_throw_on_type_mismatch = 0;

DROP TABLE IF EXISTS tvk_l;
DROP TABLE IF EXISTS tvk_r;

CREATE TABLE tvk_l (id Int32, v Variant(Int32, String), x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tvk_r (id Int32, v Variant(Int32, String), x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tvk_l VALUES (1, 5, 1, 10), (2, 'abc', 2, 20), (3, NULL, 3, 10);
INSERT INTO tvk_r VALUES (1, 3, 0, 15), (2, 'zzz', 1, 25);

-- Variant keys: not routed (compareAt would order NULL above 3 and 'abc' above 3, SQL says both comparisons are NULL).
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

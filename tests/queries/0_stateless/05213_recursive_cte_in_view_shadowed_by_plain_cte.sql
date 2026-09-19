-- ApplyWithSubqueryVisitor rewrites a CTE reference into a copy of the element's body, and CREATE VIEW stores
-- the rewritten definition. A copy taken from a WITH RECURSIVE list used to arrive without any record of that,
-- so the copy's self-reference was bound by ordinary name lookup instead of to the copy itself: unshadowed it
-- happened to find the real recursive CTE, but shadowed by a same-named plain CTE it found the plain one, whose
-- rows were already transformed, and the recursive member's filter then discarded all of them. The view was
-- left with its seed row alone while the same text run live was correct.

SET enable_analyzer = 1;

-- Every arm prints the live query and the stored view beside each other, as groupArray over an ORDER BY-ed
-- read, so an arm cannot pass on a wrong answer that both sides happen to share.

DROP VIEW IF EXISTS v_shadowed;
DROP VIEW IF EXISTS v_shadowed_union;
DROP VIEW IF EXISTS v_shadowed_in;
DROP VIEW IF EXISTS v_shadowed_twice;
DROP VIEW IF EXISTS v_renamed;
DROP VIEW IF EXISTS v_direct;
DROP VIEW IF EXISTS v_qualified;
DROP VIEW IF EXISTS v_mixed_list;
DROP VIEW IF EXISTS v_plain_cte;
DROP VIEW IF EXISTS v_in_argument;
DROP VIEW IF EXISTS v_in_inside_body;
DROP VIEW IF EXISTS mv_shadowed;

CREATE VIEW v_shadowed AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM src);

SELECT 'the recursive CTE is shadowed by a same-named plain CTE';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM src) ORDER BY id)) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_shadowed ORDER BY id)) AS view;

-- The visitor runs again on every metadata load, so the stored definition must survive a reload.
DETACH TABLE v_shadowed;
ATTACH TABLE v_shadowed;
SELECT 'the same view after a reload';
SELECT groupArray(id) FROM (SELECT id FROM v_shadowed ORDER BY id);

CREATE VIEW v_shadowed_union AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src UNION ALL SELECT 999) SELECT id FROM src);

SELECT 'the shadowing plain CTE is itself a UNION';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src UNION ALL SELECT 999) SELECT id FROM src) ORDER BY id)) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_shadowed_union ORDER BY id)) AS view;

CREATE VIEW v_shadowed_in AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT number AS id FROM numbers(100) WHERE number IN src);

SELECT 'the shadowing plain CTE is consumed by IN';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT number AS id FROM numbers(100) WHERE number IN src) ORDER BY id)) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_shadowed_in ORDER BY id)) AS view;

CREATE VIEW v_shadowed_twice AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM (WITH src AS (SELECT id * 2 AS id FROM src) SELECT id FROM src));

SELECT 'the name is shadowed at two nesting levels';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM (WITH src AS (SELECT id * 2 AS id FROM src) SELECT id FROM src)) ORDER BY id)) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_shadowed_twice ORDER BY id)) AS view;

CREATE VIEW v_renamed AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH other AS (SELECT id * 10 AS id FROM src) SELECT id FROM other);

SELECT 'control: the inner CTE has another name, so nothing is shadowed';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM (WITH other AS (SELECT id * 10 AS id FROM src) SELECT id FROM other) ORDER BY id)) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_renamed ORDER BY id)) AS view;

CREATE VIEW v_direct AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM src;

CREATE VIEW v_qualified AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT src.id AS id FROM src;

SELECT 'control: the recursive CTE is read directly, unqualified and qualified';
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM v_direct ORDER BY id)) AS unqualified,
    (SELECT groupArray(id) FROM (SELECT id FROM v_qualified ORDER BY id)) AS qualified;

CREATE VIEW v_mixed_list AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3), plain AS (SELECT 100 AS base)
SELECT base + id AS id FROM src, plain;

SELECT 'control: a non-recursive sibling in the same WITH RECURSIVE list';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3), plain AS (SELECT 100 AS base)
        SELECT base + id AS id FROM src, plain ORDER BY id)) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_mixed_list ORDER BY id)) AS view;

-- A stored definition holds the expanded bodies, so reading it does not depend on the reader still allowing a
-- name to be looked up in an enclosing scope. That is how a plain CTE in a view has always behaved; the
-- recursive one now behaves the same, because the copy binds its self-reference to itself.
CREATE VIEW v_plain_cte AS
WITH a AS (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT id FROM (WITH a AS (SELECT id * 10 AS id FROM a) SELECT id FROM a);

SELECT 'the stored definition is self-contained: read with enable_global_with_statement = 0';
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM v_shadowed ORDER BY id) SETTINGS enable_global_with_statement = 0) AS recursive_cte,
    (SELECT groupArray(id) FROM (SELECT id FROM v_plain_cte ORDER BY id) SETTINGS enable_global_with_statement = 0) AS plain_cte;

SELECT 'the view is expanded into the calling query instead: analyzer_inline_views = 1';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM src) ORDER BY id) SETTINGS analyzer_inline_views = 1) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_shadowed ORDER BY id) SETTINGS analyzer_inline_views = 1) AS view;

-- A CTE body is copied in place, so no table name enters the definition and the view depends on nothing.
CREATE MATERIALIZED VIEW mv_shadowed ENGINE = Memory AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM src);

SELECT 'neither view loads a dependency';
SELECT name, loading_dependencies_table FROM system.tables
WHERE database = currentDatabase() AND name IN ('v_shadowed', 'mv_shadowed') ORDER BY name;

-- Boundaries. A recursive CTE that is not a UNION ALL is unsupported, and storing one must be refused exactly
-- as running it is: a copy that is recognized as recursive reaches the same check.
SELECT 'boundary: an EXCEPT body is refused live and when stored';
WITH RECURSIVE src AS (SELECT 1 AS id EXCEPT SELECT id + 1 FROM src WHERE id < 3) SELECT id FROM src; -- { serverError UNSUPPORTED_METHOD }
CREATE VIEW v_except AS
WITH RECURSIVE src AS (SELECT 1 AS id EXCEPT SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM src); -- { serverError UNSUPPORTED_METHOD }

SELECT 'boundary: a UNION DISTINCT body is refused live and when stored';
WITH RECURSIVE src AS (SELECT 1 AS id UNION DISTINCT SELECT id + 1 FROM src WHERE id < 3) SELECT id FROM src; -- { serverError UNSUPPORTED_METHOD }
CREATE VIEW v_distinct AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION DISTINCT SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM src); -- { serverError UNSUPPORTED_METHOD }

SELECT 'boundary: the old analyzer refuses WITH RECURSIVE, live and stored';
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src) SELECT id FROM src) SETTINGS enable_analyzer = 0; -- { serverError UNSUPPORTED_METHOD }
SELECT id FROM v_shadowed SETTINGS enable_analyzer = 0; -- { serverError UNSUPPORTED_METHOD }

-- A recursive CTE cannot be the right argument of IN in a live query: expression resolution drops the CTE
-- marking before the union is resolved, so the self-reference falls through to the catalog. A stored
-- definition returns rows there because the copy's self-reference reaches the list it was copied from. This
-- divergence predates the fix and is left as it is; the arm exists to show that expression position is
-- untouched.
CREATE VIEW v_in_argument AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT number AS id FROM numbers(10) WHERE number IN src;

SELECT 'boundary: a recursive CTE as the right argument of IN';
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT number AS id FROM numbers(10) WHERE number IN src; -- { serverError UNKNOWN_TABLE }
SELECT groupArray(id) FROM (SELECT id FROM v_in_argument ORDER BY id);

CREATE VIEW v_in_inside_body AS
WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src WHERE id IN src) SELECT id FROM src);

SELECT 'boundary: IN reached from inside the shadowing CTE body';
SELECT
    (SELECT groupArray(id) FROM (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3)
        SELECT id FROM (WITH src AS (SELECT id * 10 AS id FROM src WHERE id IN src) SELECT id FROM src) ORDER BY id)) AS live,
    (SELECT groupArray(id) FROM (SELECT id FROM v_in_inside_body ORDER BY id)) AS view;

DROP VIEW v_shadowed;
DROP VIEW v_shadowed_union;
DROP VIEW v_shadowed_in;
DROP VIEW v_shadowed_twice;
DROP VIEW v_renamed;
DROP VIEW v_direct;
DROP VIEW v_qualified;
DROP VIEW v_mixed_list;
DROP VIEW v_plain_cte;
DROP VIEW v_in_argument;
DROP VIEW v_in_inside_body;
DROP VIEW mv_shadowed;

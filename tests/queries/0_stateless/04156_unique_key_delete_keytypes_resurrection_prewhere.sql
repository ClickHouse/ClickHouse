-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY synchronous DELETE: resurrection (re-INSERT of a deleted key wins
-- by greater _block_number), String key, compound UNIQUE KEY, and PREWHERE over
-- a table with deletes (the read-path bitmap filter must hide dead rows).
-- All keys distinct (dedup is a later PR); merges stopped.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

-- ============================================================================
-- 1. Resurrection: INSERT -> DELETE a key -> re-INSERT the same key. The new
--    row has a strictly-greater _block_number and wins; the old bitmap-dead row
--    stays invisible.
-- ============================================================================
DROP TABLE IF EXISTS uk_resurrect;

CREATE TABLE uk_resurrect (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_resurrect;

INSERT INTO uk_resurrect VALUES (1, 'old1'), (2, 'old2'), (3, 'old3');
SELECT 'res_after_insert' AS step, count() FROM uk_resurrect;  -- 3

-- Kill id = 2.
DELETE FROM uk_resurrect WHERE id = 2;
SELECT 'res_after_delete' AS step, count() FROM uk_resurrect;  -- 2
SELECT 'res_v_for_2' AS step, count() FROM uk_resurrect WHERE id = 2;  -- 0

-- Re-INSERT id = 2 with a new value; the newer row supersedes the dead one.
INSERT INTO uk_resurrect VALUES (2, 'new2');
SELECT 'res_after_reinsert' AS step, count() FROM uk_resurrect;  -- 3
-- The survivor for id = 2 must be the new value, exactly once.
SELECT 'res_value_2' AS step, id, v FROM uk_resurrect WHERE id = 2 ORDER BY id;  -- 2 new2
SELECT 'res_survivors' AS step, id, v FROM uk_resurrect ORDER BY id;  -- 1 old1 / 2 new2 / 3 old3

DROP TABLE uk_resurrect;

-- ============================================================================
-- 2. String UNIQUE KEY.
-- ============================================================================
DROP TABLE IF EXISTS uk_str;

CREATE TABLE uk_str (k String, n UInt32)
ENGINE = MergeTree
UNIQUE KEY (k)
ORDER BY (k);

SYSTEM STOP MERGES uk_str;

INSERT INTO uk_str VALUES ('alpha', 1), ('bravo', 2), ('charlie', 3), ('delta', 4);
SELECT 'str_after_insert' AS step, count() FROM uk_str;  -- 4

DELETE FROM uk_str WHERE k = 'bravo';
DELETE FROM uk_str WHERE k LIKE 'd%';  -- kills 'delta'
SELECT 'str_after_delete' AS step, count() FROM uk_str;  -- 2
SELECT 'str_survivors' AS step, k, n FROM uk_str ORDER BY k;  -- alpha 1 / charlie 3

DROP TABLE uk_str;

-- ============================================================================
-- 3. Compound UNIQUE KEY (multi-column).
-- ============================================================================
DROP TABLE IF EXISTS uk_compound;

CREATE TABLE uk_compound (a UInt32, b String, v String)
ENGINE = MergeTree
UNIQUE KEY (a, b)
ORDER BY (a, b);

SYSTEM STOP MERGES uk_compound;

-- Distinct compound keys (note (1,'x') vs (1,'y') vs (2,'x') are all distinct).
INSERT INTO uk_compound VALUES
    (1, 'x', 'r1'), (1, 'y', 'r2'), (2, 'x', 'r3'), (2, 'y', 'r4');
SELECT 'cmp_after_insert' AS step, count() FROM uk_compound;  -- 4

-- DELETE one compound key (a = 1 AND b = 'y') and a whole-column predicate.
DELETE FROM uk_compound WHERE a = 1 AND b = 'y';  -- kills (1,'y')
SELECT 'cmp_after_delete1' AS step, count() FROM uk_compound;  -- 3

DELETE FROM uk_compound WHERE b = 'x';  -- kills (1,'x') and (2,'x')
SELECT 'cmp_after_delete2' AS step, count() FROM uk_compound;  -- 1
SELECT 'cmp_survivors' AS step, a, b, v FROM uk_compound ORDER BY a, b;  -- 2 y r4

DROP TABLE uk_compound;

-- ============================================================================
-- 4. PREWHERE over a table with deletes: rows killed by the bitmap must not
--    appear (exercises the read-path filter under PREWHERE / the
--    query-condition-cache path).
-- ============================================================================
DROP TABLE IF EXISTS uk_prewhere;

CREATE TABLE uk_prewhere (id UInt64, grp UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_prewhere;

INSERT INTO uk_prewhere VALUES
    (1, 100, 'a'), (2, 100, 'b'), (3, 200, 'c'),
    (4, 200, 'd'), (5, 100, 'e'), (6, 200, 'f');
SELECT 'pw_after_insert' AS step, count() FROM uk_prewhere;  -- 6

-- Delete some rows that the PREWHERE predicate would otherwise select.
DELETE FROM uk_prewhere WHERE id IN (2, 4);
SELECT 'pw_after_delete' AS step, count() FROM uk_prewhere;  -- 4

-- PREWHERE grp = 100: matching rows are 1,2,5 but 2 is dead -> 1,5.
SELECT 'pw_grp100' AS step, id, v FROM uk_prewhere PREWHERE grp = 100 ORDER BY id;  -- 1 a / 5 e
-- PREWHERE grp = 200: matching rows are 3,4,6 but 4 is dead -> 3,6.
SELECT 'pw_grp200' AS step, id, v FROM uk_prewhere PREWHERE grp = 200 ORDER BY id;  -- 3 c / 6 f
-- PREWHERE that targets a dead row directly returns nothing.
SELECT 'pw_dead' AS step, count() FROM uk_prewhere PREWHERE id = 4;  -- 0

DROP TABLE uk_prewhere;

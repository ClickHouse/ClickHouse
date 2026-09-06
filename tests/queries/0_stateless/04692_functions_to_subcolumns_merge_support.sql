-- One query visits the same Merge storage from many nodes; the support answer is memoized per
-- pass run, so assert the memoized answer equals the fresh one on both the true and false arm.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS mt1;
DROP TABLE IF EXISTS mt2;
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS m_all;
DROP TABLE IF EXISTS m_mixed;
DROP TABLE IF EXISTS m_empty;
DROP TABLE IF EXISTS m_nested;

CREATE TABLE mt1 (id UInt64, arr Array(UInt64), s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mt2 (id UInt64, arr Array(UInt64), s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO mt1 VALUES (1, [1, 2, 3], 'abc');
INSERT INTO mt2 VALUES (2, [4, 5], 'xyz');

CREATE TABLE m_all AS mt1 ENGINE = Merge(currentDatabase(), '^mt[0-9]+$');

-- Only MergeTree children: the rewrite fires. `position` is not rewritable, so the same query
-- reaches the support check from both a rewritable and a non-rewritable node.
SELECT 'merge over MergeTree, rewritten', count() > 0
FROM (EXPLAIN QUERY TREE SELECT length(arr), position(s, 'b') FROM m_all)
WHERE explain ILIKE '%size0%';

SELECT 'merge over MergeTree, values', length(arr), position(s, 'b') FROM m_all ORDER BY 2, 3;

-- A File child opts out of the optimization, so the Merge must fail closed.
CREATE TABLE f1 (id UInt64, arr Array(UInt64), s String) ENGINE = File(CSV);
INSERT INTO f1 VALUES (3, [7], 'q');
CREATE TABLE m_mixed AS mt1 ENGINE = Merge(currentDatabase(), '^(mt1|f1)$');

SELECT 'file child, not rewritten', count()
FROM (EXPLAIN QUERY TREE SELECT length(arr) FROM f1)
WHERE explain ILIKE '%size0%';

SELECT 'merge with file child, not rewritten', count()
FROM (EXPLAIN QUERY TREE SELECT length(arr), position(s, 'b') FROM m_mixed)
WHERE explain ILIKE '%size0%';

SELECT 'merge with file child, values', length(arr), position(s, 'b') FROM m_mixed ORDER BY 2, 3;

-- Two storages with different answers in one query. The memoized answer must stay per storage:
-- exactly one side is rewritten, in either order. A memo that returns the first answer for every
-- storage gives 2 here and 0 below.
SELECT 'two storages, supporting side first', count()
FROM (EXPLAIN QUERY TREE SELECT length(arr) FROM mt1 UNION ALL SELECT length(arr) FROM m_mixed)
WHERE explain ILIKE '%size0%';

SELECT 'two storages, opting-out side first', count()
FROM (EXPLAIN QUERY TREE SELECT length(arr) FROM m_mixed UNION ALL SELECT length(arr) FROM mt1)
WHERE explain ILIKE '%size0%';

-- A File storage answers the two support predicates differently: it refuses every transformer
-- except tuple element access. The two answers must stay separate per storage, so in one query
-- the tuple element is rewritten and length() is not, whichever node is visited first.
CREATE TABLE f2 (id UInt64, t Tuple(a UInt64, b UInt64), arr Array(UInt64)) ENGINE = File(CSV);
INSERT INTO f2 VALUES (1, (10, 20), [1, 2, 3]);

SELECT 'tuple element first, tuple rewritten', count()
FROM (EXPLAIN QUERY TREE SELECT tupleElement(t, 'a'), length(arr) FROM f2)
WHERE explain ILIKE '%t.a%';

SELECT 'tuple element first, length not rewritten', count()
FROM (EXPLAIN QUERY TREE SELECT tupleElement(t, 'a'), length(arr) FROM f2)
WHERE explain ILIKE '%size0%';

SELECT 'length first, tuple rewritten', count()
FROM (EXPLAIN QUERY TREE SELECT length(arr), tupleElement(t, 'a') FROM f2)
WHERE explain ILIKE '%t.a%';

SELECT 'length first, length not rewritten', count()
FROM (EXPLAIN QUERY TREE SELECT length(arr), tupleElement(t, 'a') FROM f2)
WHERE explain ILIKE '%size0%';

SELECT 'mixed capability, values', tupleElement(t, 'a'), length(arr) FROM f2;

-- An empty match set has no child that opts out, so the answer is vacuously true.
CREATE TABLE m_empty AS mt1 ENGINE = Merge(currentDatabase(), '^nomatch_zzz$');

SELECT 'empty match set, rewritten', count() > 0
FROM (EXPLAIN QUERY TREE SELECT length(arr) FROM m_empty)
WHERE explain ILIKE '%size0%';

SELECT 'empty match set, values', count() FROM m_empty;

-- A Merge whose child is itself a Merge: the recursion happens inside the child's own support
-- check, below the memoized outer answer.
CREATE TABLE m_nested AS mt1 ENGINE = Merge(currentDatabase(), '^m_all$');

SELECT 'nested merge, rewritten', count() > 0
FROM (EXPLAIN QUERY TREE SELECT length(arr), position(s, 'b') FROM m_nested)
WHERE explain ILIKE '%size0%';

SELECT 'nested merge, values', length(arr), position(s, 'b') FROM m_nested ORDER BY 2, 3;

DROP TABLE m_nested;
DROP TABLE m_empty;
DROP TABLE m_mixed;
DROP TABLE m_all;
DROP TABLE f2;
DROP TABLE f1;
DROP TABLE mt2;
DROP TABLE mt1;

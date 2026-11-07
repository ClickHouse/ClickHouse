-- Tags: no-old-analyzer

-- A right side that a physical join would look up in place - a `Join` engine table, a dictionary -
-- is read as an ordinary stream by the block nested loop join, so it is the pre-join actions that
-- make its columns `Nullable` under `join_use_nulls`, not the lookup. Getting that wrong padded the
-- unmatched rows with type defaults inside a column declared `Nullable`, and any expression over
-- such a column failed.

SET enable_analyzer = 1;
SET cross_to_inner_join_rewrite = 0;

DROP DICTIONARY IF EXISTS bnl_ps_dict;
DROP TABLE IF EXISTS bnl_ps_source;
DROP TABLE IF EXISTS bnl_ps_join;

CREATE TABLE bnl_ps_join (k UInt64, val String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO bnl_ps_join VALUES (1, 'x'), (2, 'y'), (3, 'z');

CREATE TABLE bnl_ps_source (k UInt64, val String) ENGINE = MergeTree ORDER BY k;
INSERT INTO bnl_ps_source VALUES (1, 'x'), (2, 'y'), (3, 'z');
CREATE DICTIONARY bnl_ps_dict (k UInt64, val String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'bnl_ps_source')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

SET join_use_nulls = 1;

SELECT 'join engine', l.k, r.k, r.val, r.val IS NULL
FROM (SELECT number AS k FROM numbers(4)) l LEFT JOIN bnl_ps_join r ON l.k < r.k ORDER BY ALL;

SELECT 'join engine expression', l.k, r.k + 1
FROM (SELECT number AS k FROM numbers(4)) l LEFT JOIN bnl_ps_join r ON l.k < r.k ORDER BY ALL;

SELECT 'join engine full', l.k, r.k, r.val
FROM (SELECT number + 2 AS k FROM numbers(3)) l FULL JOIN bnl_ps_join r ON l.k < r.k ORDER BY ALL;

SELECT 'dictionary', l.k, r.k, r.val, r.val IS NULL
FROM (SELECT number AS k FROM numbers(4)) l LEFT JOIN bnl_ps_dict r ON l.k < r.k ORDER BY ALL;

SELECT 'dictionary expression', l.k, r.k + 1
FROM (SELECT number AS k FROM numbers(4)) l LEFT JOIN bnl_ps_dict r ON l.k < r.k ORDER BY ALL;

SET join_use_nulls = 0;

SELECT 'join engine no use_nulls', l.k, r.k, r.val
FROM (SELECT number AS k FROM numbers(4)) l LEFT JOIN bnl_ps_join r ON l.k < r.k ORDER BY ALL;

SELECT 'dictionary no use_nulls', l.k, r.k, r.val
FROM (SELECT number AS k FROM numbers(4)) l LEFT JOIN bnl_ps_dict r ON l.k < r.k ORDER BY ALL;

-- The lookup itself is untouched: an equality still reaches the storage as a filled join.
SELECT 'lookup kept', l.k, r.val
FROM (SELECT number AS k FROM numbers(4)) l ANY LEFT JOIN bnl_ps_join r ON l.k = r.k ORDER BY ALL;

DROP DICTIONARY bnl_ps_dict;
DROP TABLE bnl_ps_source;
DROP TABLE bnl_ps_join;

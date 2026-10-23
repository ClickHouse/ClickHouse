-- Tags: distributed

-- The child query StorageMerge builds for each matched table has its GROUP BY and ORDER BY cleared
-- because they may reference the removed JOIN. A `Distributed` child re-validates that query tree, so a
-- GROUP BY modifier left behind on it made the whole SELECT fail with NOT_IMPLEMENTED. One local shard
-- is enough: the rejection happens on the initiator while planning the child.

DROP TABLE IF EXISTS t_99508;
DROP TABLE IF EXISTS d_99508;
DROP TABLE IF EXISTS m_99508;
DROP TABLE IF EXISTS mt_99508;
DROP TABLE IF EXISTS r_99508;

CREATE TABLE t_99508 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_99508 SELECT number % 3, number FROM numbers(9);
CREATE TABLE d_99508 (a UInt32, b UInt32)
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_99508);
CREATE TABLE m_99508  (a UInt32, b UInt32) ENGINE = Merge(currentDatabase(), '^d_99508$');
CREATE TABLE mt_99508 (a UInt32, b UInt32) ENGINE = Merge(currentDatabase(), '^t_99508$');
CREATE TABLE r_99508 (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO r_99508 SELECT number FROM numbers(3);

SELECT '-- 1 witness: Merge over Distributed + JOIN + WITH TOTALS';
SELECT m.a, count() FROM m_99508 AS m JOIN r_99508 AS r ON m.a = r.a GROUP BY m.a WITH TOTALS ORDER BY m.a;

SELECT '-- 2 witness: the other half of the same rejection, WITH ROLLUP';
-- group_by_use_nulls is pinned on the arms with a defaultable grouping key: the stress runner turns it
-- on in half its workers (ci/jobs/scripts/stress/stress.py), and it renders the ROLLUP/CUBE/GROUPING
-- SETS subtotal key as NULL instead of 0. It does not affect WITH TOTALS, so those arms stay unpinned.
SELECT m.a, count() FROM m_99508 AS m JOIN r_99508 AS r ON m.a = r.a GROUP BY m.a WITH ROLLUP
ORDER BY m.a, count() SETTINGS group_by_use_nulls = 0;

SELECT '-- 3 witness: ARRAY JOIN reaches the same rewrite';
SELECT a, count() FROM m_99508 LEFT ARRAY JOIN [1, 2] AS z GROUP BY a WITH TOTALS ORDER BY a;

SELECT '-- 4 witness: WITH CUBE';
SELECT m.a, count() FROM m_99508 AS m JOIN r_99508 AS r ON m.a = r.a GROUP BY m.a WITH CUBE
ORDER BY m.a, count() SETTINGS group_by_use_nulls = 0;

SELECT '-- 5 witness: GROUPING SETS reaches the same rejection';
-- prefer_localhost_replica is pinned on this arm alone: with it off the child query reaches the shard as
-- formatted SQL, where GROUPING SETS is printed only while the GROUP BY list it was cleared from is
-- still present, so the modifier is dropped in transit and this arm cannot fail.
SELECT m.a, count() FROM m_99508 AS m JOIN r_99508 AS r ON m.a = r.a
GROUP BY GROUPING SETS ((m.a), ()) ORDER BY m.a, count()
SETTINGS group_by_use_nulls = 0, prefer_localhost_replica = 1;

SELECT '-- 6 oracle and control: the same rows over a Merge of the plain table';
SELECT m.a, count() FROM mt_99508 AS m JOIN r_99508 AS r ON m.a = r.a GROUP BY m.a WITH TOTALS ORDER BY m.a;

SELECT '-- 7 control: the old analyzer already reset the modifiers';
SELECT m.a, count() FROM m_99508 AS m JOIN r_99508 AS r ON m.a = r.a GROUP BY m.a WITH TOTALS ORDER BY m.a
SETTINGS enable_analyzer = 0;

SELECT '-- 8 control: without a JOIN the child query is not rewritten';
SELECT a, count() FROM m_99508 GROUP BY a WITH TOTALS ORDER BY a;

DROP TABLE t_99508;
DROP TABLE d_99508;
DROP TABLE m_99508;
DROP TABLE mt_99508;
DROP TABLE r_99508;

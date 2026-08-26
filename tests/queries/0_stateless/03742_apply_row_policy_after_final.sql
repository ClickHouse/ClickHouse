-- Test for apply_row_policy_after_final setting with ReplacingMergeTree, https://github.com/ClickHouse/ClickHouse/issues/90986

DROP TABLE IF EXISTS tab;
DROP ROW POLICY IF EXISTS pol1 ON tab;

SET enable_analyzer = 1;

CREATE TABLE tab (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab VALUES (1, 'ccc', 2);

SELECT '= no row policy =';
SET apply_row_policy_after_final = 0;
SELECT '--- raw';
SELECT * FROM tab ORDER BY x, version;
SELECT '--- final';
SELECT * FROM tab FINAL ORDER BY x;

-- Create row policy that filters out rows where y = 'ccc'
CREATE ROW POLICY pol1 ON tab USING y != 'ccc' TO ALL;

SELECT '= with row policy (default behavior - filter before FINAL) =';
SELECT '--- raw';
SELECT * FROM tab ORDER BY x, version;
SELECT '--- final';
SELECT * FROM tab FINAL ORDER BY x;

SELECT '= with row policy and apply_row_policy_after_final =';
SET apply_row_policy_after_final = 1;
SELECT '--- raw';
SELECT * FROM tab ORDER BY x, version;
SELECT '--- final';
-- now the row with x=1, y='ccc', version=2 should be selected by FINAL first,
-- then filtered out by the row policy, leaving only x=2
SELECT * FROM tab FINAL ORDER BY x;

DROP ROW POLICY pol1 ON tab;
DROP TABLE tab;

-- Test that filter on ORDER BY column is still applied before FINAL (optimization)
SELECT '';
SELECT '= row policy on ORDER BY column should be applied before FINAL =';

DROP TABLE IF EXISTS tab2;
DROP ROW POLICY IF EXISTS pol2 ON tab2;

CREATE TABLE tab2 (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab2 VALUES (1, 'aaa', 1), (2, 'bbb', 1), (3, 'ccc', 1);
INSERT INTO tab2 VALUES (1, 'ddd', 2);

-- row policy on x (ORDER BY column) - should be applied before FINAL
CREATE ROW POLICY pol2 ON tab2 USING x != 1 TO ALL;

SET apply_row_policy_after_final = 1;
SELECT '--- final with policy on ORDER BY column';
-- both rows with x=1 should be filtered out before FINAL
SELECT * FROM tab2 FINAL ORDER BY x;

DROP ROW POLICY pol2 ON tab2;
DROP TABLE tab2;

-- test that PREWHERE is also deferred when row policy is deferred
SELECT '';
SELECT '= PREWHERE should be deferred together with row policy =';

DROP TABLE IF EXISTS tab3;
DROP ROW POLICY IF EXISTS pol3 ON tab3;

CREATE TABLE tab3 (x UInt32, y String, z UInt32, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab3 VALUES (1, 'aaa', 100, 1), (2, 'bbb', 200, 1);
INSERT INTO tab3 VALUES (1, 'ccc', 300, 2), (2, 'ddd', 300, 2);

CREATE ROW POLICY pol3 ON tab3 USING y != 'ccc' TO ALL;

SELECT '--- without setting';
SET apply_row_policy_after_final = 0;

-- PREWHERE z < 250 filters (1, 'ccc', 300, 2) and (2, 'ddd', 300, 2)
-- FINAL gets (1, 'aaa', 100, 1) and (2, 'bbb', 200, 1)
SELECT * FROM tab3 FINAL PREWHERE z < 250 ORDER BY x;

SELECT '--- with setting';
SET apply_row_policy_after_final = 1;
-- FINAL first: (1, 'ccc', 300, 2) and (2, 'ddd', 300, 2)
-- row policy removes (1, 'ccc', 300, 2)
-- PREWHERE z < 250: (2, 'ddd', 300, 2) does not pass, so nothing
SELECT * FROM tab3 FINAL PREWHERE z < 250 ORDER BY x;

DROP ROW POLICY pol3 ON tab3;
DROP TABLE tab3;

-- Test that SELECT with subset of columns works when row policy uses other columns
SELECT '';
SELECT '= SELECT subset of columns with row policy on other column =';

DROP TABLE IF EXISTS tab4;
DROP ROW POLICY IF EXISTS pol4 ON tab4;

CREATE TABLE tab4 (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab4 VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab4 VALUES (1, 'ccc', 2);

-- Row policy on y, but we SELECT only x
CREATE ROW POLICY pol4 ON tab4 USING y != 'ccc' TO ALL;

SET apply_row_policy_after_final = 1;
SELECT '--- SELECT x only (row policy on y should still work)';
SELECT x FROM tab4 FINAL ORDER BY x;

SELECT '--- SELECT x, version (row policy on y should still work)';
SELECT x, version FROM tab4 FINAL ORDER BY x;

DROP ROW POLICY pol4 ON tab4;
DROP TABLE tab4;

DROP TABLE IF EXISTS tab_final;
DROP ROW POLICY IF EXISTS pol_final ON tab_final;

CREATE TABLE tab_final (x UInt32, y String, version UInt32) ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_final VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab_final VALUES (1, 'ccc', 2);

SET apply_prewhere_after_final = 1;
SELECT '--- PREWHERE after FINAL';
SELECT x FROM tab_final FINAL PREWHERE y != 'ccc' ORDER BY x;

SET apply_prewhere_after_final = 0;
SELECT '--- PREWHERE before FINAL';
SELECT x FROM tab_final FINAL PREWHERE y != 'ccc' ORDER BY x;

DROP TABLE tab_final;

-- test that partition pruning works OK with row policy
-- When row policy column in the partition key, minmax index must not prune partitions that FINAL needs for correct deduplication
SELECT '';

DROP TABLE IF EXISTS tab_part;
DROP ROW POLICY IF EXISTS pol_part ON tab_part;

CREATE TABLE tab_part (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) PARTITION BY y ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_part VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab_part VALUES (1, 'ccc', 2);

CREATE ROW POLICY pol_part ON tab_part USING y != 'ccc' TO ALL;

SET apply_row_policy_after_final = 0;
SELECT '--- filter before FINAL';
-- partition 'ccc' pruned, FINAL sees (1,'aaa',1) and (2,'bbb',1)
SELECT * FROM tab_part FINAL ORDER BY x;

SET apply_row_policy_after_final = 1;
SELECT '--- deferred';
-- all partitions read, FINAL picks (1,'ccc',2) then row policy removes it
SELECT * FROM tab_part FINAL ORDER BY x;

DROP ROW POLICY pol_part ON tab_part;
DROP TABLE tab_part;

SELECT '';

SELECT '= WHERE + FINAL must not prune partitions with non-sorting-key partition columns =';

DROP TABLE IF EXISTS tab_where;

CREATE TABLE tab_where (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) PARTITION BY y ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_where VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab_where VALUES (1, 'ccc', 2), (2, 'ddd', 2);

SELECT '--- FINAL WHERE y != ccc (should see deduplication winners, then filter)';
SELECT * FROM tab_where FINAL WHERE y != 'ccc' ORDER BY x;

DROP TABLE tab_where;

SELECT '';
SELECT '= PARTITION BY sorting-key column is still prunable =';

DROP TABLE IF EXISTS tab_safe;

CREATE TABLE tab_safe (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) PARTITION BY x ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_safe VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab_safe VALUES (1, 'ccc', 2);

SELECT '--- FINAL WHERE x != 1 (partition pruning is safe here)';
SELECT * FROM tab_safe FINAL WHERE x != 1 ORDER BY x;

DROP TABLE tab_safe;

SELECT '';

SELECT '= row policy on toDate(time) with ORDER BY toDate(time) — prewhere should NOT be deferred =';

DROP TABLE IF EXISTS tab_todate_policy;
DROP ROW POLICY IF EXISTS pol_todate ON tab_todate_policy;

CREATE TABLE tab_todate_policy (time DateTime, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY toDate(time) SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_todate_policy VALUES ('2024-01-01 10:00:00', 'aaa', 1), ('2024-01-02 12:00:00', 'bbb', 1);
INSERT INTO tab_todate_policy VALUES ('2024-01-01 11:00:00', 'ccc', 2), ('2024-01-02 13:00:00', 'ddd', 2);

CREATE ROW POLICY pol_todate ON tab_todate_policy USING toDate(time) = '2024-01-01' TO ALL;

SET apply_row_policy_after_final = 1;
-- rp is over sorting key toDate(time), so neither row policy nor prewhere should be deferred
SELECT '--- toDate(time) row policy: neither row policy nor prewhere deferred';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab_todate_policy FINAL PREWHERE y != 'ddd' ORDER BY time) WHERE explain LIKE '%Deferred%' SETTINGS enable_analyzer=1;

DROP ROW POLICY pol_todate ON tab_todate_policy;
SET apply_row_policy_after_final = 0;
DROP TABLE tab_todate_policy;
SELECT '= compound row policy: sorting-key atom should be used for index analysis =';

DROP TABLE IF EXISTS tab_compound;
DROP ROW POLICY IF EXISTS pol_compound ON tab_compound;

CREATE TABLE tab_compound (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_compound VALUES (1, 'aaa', 1), (2, 'bbb', 1), (3, 'ccc', 1);
INSERT INTO tab_compound VALUES (1, 'ddd', 2), (2, 'eee', 2);

CREATE ROW POLICY pol_compound ON tab_compound USING y != 'ddd' AND x > 1 TO ALL;

SET apply_row_policy_after_final = 1;
SELECT '--- FINAL: x>1 atom should still participate in primary key analysis';
-- FINAL has (1,'ddd',2), (2,'eee',2), (3,'ccc',1)
-- row policy y!='ddd' AND x>1 -> (2,'eee',2), (3,'ccc',1)
SELECT * FROM tab_compound FINAL ORDER BY x;

SELECT '--- EXPLAIN indexes: x > 1 should appear in PrimaryKey condition';
EXPLAIN indexes = 1 SELECT * FROM tab_compound FINAL ORDER BY x FORMAT TabSeparated;

DROP ROW POLICY pol_compound ON tab_compound;
SET apply_row_policy_after_final = 0;
DROP TABLE tab_compound;

SELECT '';
SELECT '= compound PREWHERE: sorting-key atom should be used for index analysis =';

DROP TABLE IF EXISTS tab_compound_pw;

CREATE TABLE tab_compound_pw (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_compound_pw VALUES (1, 'aaa', 1), (2, 'bbb', 1), (3, 'ccc', 1);
INSERT INTO tab_compound_pw VALUES (1, 'ddd', 2), (2, 'eee', 2);

SET apply_prewhere_after_final = 1;
SELECT '--- FINAL PREWHERE y != ddd AND x > 1: x>1 atom should still participate in primary key analysis';
-- FINAL has (1,'ddd',2), (2,'eee',2), (3,'ccc',1)
-- PREWHERE y!='ddd' AND x>1 -> (2,'eee',2), (3,'ccc',1)
SELECT * FROM tab_compound_pw FINAL PREWHERE y != 'ddd' AND x > 1 ORDER BY x;

SELECT '--- EXPLAIN indexes: x > 1 should appear in PrimaryKey condition';
EXPLAIN indexes = 1 SELECT * FROM tab_compound_pw FINAL PREWHERE y != 'ddd' AND x > 1 ORDER BY x FORMAT TabSeparated;

SELECT '--- EXPLAIN actions: prewhere should be deferred';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab_compound_pw FINAL PREWHERE y != 'ddd' AND x > 1 ORDER BY x) WHERE explain LIKE '%Deferred%';

SET apply_prewhere_after_final = 0;
DROP TABLE tab_compound_pw;

SELECT '';
SELECT '= nested AND in row policy: both sorting-key atoms should be used for index analysis =';

DROP TABLE IF EXISTS tab_nested_and;
DROP ROW POLICY IF EXISTS pol_nested ON tab_nested_and;

CREATE TABLE tab_nested_and (x UInt32, y String, z UInt32, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_nested_and VALUES (1, 'aaa', 100, 1), (2, 'bbb', 200, 1), (3, 'ccc', 300, 1), (5, 'ddd', 500, 1);
INSERT INTO tab_nested_and VALUES (1, 'eee', 150, 2), (2, 'fff', 250, 2);

CREATE ROW POLICY pol_nested ON tab_nested_and USING (y != 'eee' AND x > 1) AND x < 5 TO ALL;

SET apply_row_policy_after_final = 1;
SELECT '--- FINAL with nested AND row policy';
-- FINAL has (1,'eee',150,2), (2,'fff',250,2), (3,'ccc',300,1), (5,'ddd',500,1)
-- row policy (y!='eee' AND x>1) AND x<5 -> (2,'fff',250,2), (3,'ccc',300,1)
SELECT * FROM tab_nested_and FINAL ORDER BY x;

SELECT '--- EXPLAIN indexes: both x > 1 and x < 5 should appear in PrimaryKey condition';
EXPLAIN indexes = 1 SELECT * FROM tab_nested_and FINAL ORDER BY x FORMAT TabSeparated;

DROP ROW POLICY pol_nested ON tab_nested_and;
SET apply_row_policy_after_final = 0;
DROP TABLE tab_nested_and;

SELECT '';
SELECT '= nested AND in PREWHERE: both sorting-key atoms should be used for index analysis =';

DROP TABLE IF EXISTS tab_nested_and_pw;

CREATE TABLE tab_nested_and_pw (x UInt32, y String, z UInt32, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_nested_and_pw VALUES (1, 'aaa', 100, 1), (2, 'bbb', 200, 1), (3, 'ccc', 300, 1), (5, 'ddd', 500, 1);
INSERT INTO tab_nested_and_pw VALUES (1, 'eee', 150, 2), (2, 'fff', 250, 2);

SET apply_prewhere_after_final = 1;
SELECT '--- FINAL PREWHERE (y != eee AND x > 1) AND x < 5';
SELECT * FROM tab_nested_and_pw FINAL PREWHERE (y != 'eee' AND x > 1) AND x < 5 ORDER BY x;

SELECT '--- EXPLAIN indexes: both x > 1 and x < 5 should appear in PrimaryKey condition';
EXPLAIN indexes = 1 SELECT * FROM tab_nested_and_pw FINAL PREWHERE (y != 'eee' AND x > 1) AND x < 5 ORDER BY x FORMAT TabSeparated;

SELECT '--- EXPLAIN actions: prewhere should be deferred';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab_nested_and_pw FINAL PREWHERE (y != 'eee' AND x > 1) AND x < 5 ORDER BY x) WHERE explain LIKE '%Deferred%';

SET apply_prewhere_after_final = 0;
DROP TABLE tab_nested_and_pw;

SELECT '';
SELECT '= row policy on non-SK column + PREWHERE on SK column: both must be deferred =';

DROP TABLE IF EXISTS tab_sk_prewhere;
DROP ROW POLICY IF EXISTS pol_sk_pw ON tab_sk_prewhere;

CREATE TABLE tab_sk_prewhere (x UInt32, y String, deleted Int8, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO tab_sk_prewhere VALUES (1, 'aaa', 0, 1), (2, 'bbb', 0, 1), (3, 'ccc', 1, 1);
INSERT INTO tab_sk_prewhere VALUES (1, 'ddd', 1, 2), (2, 'eee', 0, 2);

CREATE ROW POLICY pol_sk_pw ON tab_sk_prewhere USING deleted = 0 TO ALL;

SET apply_row_policy_after_final = 1;

SELECT '--- data correctness: PREWHERE x = 2 with row policy deleted = 0';
-- After FINAL: (1,'ddd',1,2), (2,'eee',0,2), (3,'ccc',1,1)
-- Row policy deleted=0: (2,'eee',0,2)
-- PREWHERE x=2:        (2,'eee',0,2)
SELECT * FROM tab_sk_prewhere FINAL PREWHERE x = 2 ORDER BY x;

SELECT '--- EXPLAIN: both row policy and PREWHERE deferred';
SELECT explain FROM (EXPLAIN actions=1 SELECT * FROM tab_sk_prewhere FINAL PREWHERE x = 2 ORDER BY x) WHERE explain LIKE '%Deferred%' SETTINGS enable_analyzer=1;

DROP ROW POLICY pol_sk_pw ON tab_sk_prewhere;
SET apply_row_policy_after_final = 0;
DROP TABLE tab_sk_prewhere;

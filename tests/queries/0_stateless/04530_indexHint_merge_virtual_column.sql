-- Test: indexHint over Merge table with virtual column _table should not fail
-- https://github.com/ClickHouse/ClickHouse/issues/114013
-- { echo }

SET enable_analyzer = 1;

DROP TABLE IF EXISTS m_v1;
DROP TABLE IF EXISTS m_v2;
DROP TABLE IF EXISTS m_all;

CREATE TABLE m_v1 (key UInt32, value UInt32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE m_v2 (key UInt32, value UInt32) ENGINE = MergeTree ORDER BY key;

INSERT INTO m_v1 VALUES (1, 10);
INSERT INTO m_v2 VALUES (2, 20);

CREATE TABLE m_all (key UInt32, value UInt32) ENGINE = Merge(currentDatabase(), '^m_v[0-9]$');

SELECT _table, key FROM m_all WHERE indexHint(_table = 'm_v1') AND (_table = 'm_v1') ORDER BY key;

-- Also test with different virtual column value to ensure indexHint works correctly
SELECT _table, key FROM m_all WHERE indexHint(_table = 'm_v2') AND (_table = 'm_v2') ORDER BY key;

-- Virtual column outside indexHint should still work as before
SELECT _table, key FROM m_all WHERE _table = 'm_v1' ORDER BY key;

-- Multiple indexHint virtual columns
SELECT _table, key FROM m_all WHERE indexHint(_table = 'm_v1') AND _table = 'm_v1' AND indexHint(key = 1) ORDER BY key;

DROP TABLE IF EXISTS m_all;
DROP TABLE IF EXISTS m_v1;
DROP TABLE IF EXISTS m_v2;

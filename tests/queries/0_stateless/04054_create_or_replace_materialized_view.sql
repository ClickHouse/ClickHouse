-- Tags: no-ordinary-database

DROP TABLE IF EXISTS test_mv SYNC;
DROP TABLE IF EXISTS src SYNC;
DROP TABLE IF EXISTS target SYNC;

CREATE TABLE src (x UInt32) ENGINE = MergeTree ORDER BY x;

-- 1. First create: MV does not exist yet
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'test_mv';

-- 2. Replace with schema change (inner table case)
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x, x * 2 AS y FROM src;
SHOW CREATE TABLE test_mv;

-- Verify the pipeline works after replace
INSERT INTO src VALUES (1), (2), (3);
SELECT x, y FROM test_mv ORDER BY x;

-- 3. After replacement, exactly one inner table must exist
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%';

-- 4. TO-table case
CREATE TABLE target (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE OR REPLACE MATERIALIZED VIEW test_mv TO target AS SELECT x FROM src;
INSERT INTO src VALUES (10), (20);
SELECT * FROM target ORDER BY x;

-- 5. Switch from TO-table MV to inner-table MV
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
INSERT INTO src VALUES (100);
SELECT x FROM test_mv ORDER BY x;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%';

-- 6. Switch from inner-table MV back to TO-table MV
TRUNCATE TABLE target;
CREATE OR REPLACE MATERIALIZED VIEW test_mv TO target AS SELECT x FROM src;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%';
INSERT INTO src VALUES (200);
SELECT * FROM target ORDER BY x;

-- 7. REPLACE (without CREATE) on non-existent MV is a syntax error: the parser only handles REPLACE TABLE, not REPLACE MATERIALIZED VIEW
DROP TABLE IF EXISTS test_mv SYNC;
REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src; -- { clientError SYNTAX_ERROR }

-- 8. Replacing an inner-table MV creates a fresh inner table; pre-existing src data is not captured
-- (POPULATE is not supported in Replicated databases and is tested separately)
DROP TABLE IF EXISTS test_mv SYNC;
TRUNCATE TABLE src;
INSERT INTO src VALUES (1000), (2000), (3000);
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
-- Without POPULATE the inner table starts empty; pre-existing src rows are not captured
SELECT count() FROM test_mv;

-- After replace, only future inserts are captured; old inner table is dropped
INSERT INTO src VALUES (4000);
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
INSERT INTO src VALUES (5000);
SELECT x FROM test_mv ORDER BY x;

-- 9. Idempotency: identical CREATE OR REPLACE twice; data inserted between the two is lost (inner table is replaced)
DROP TABLE IF EXISTS test_mv SYNC;
TRUNCATE TABLE src;
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
INSERT INTO src VALUES (1);
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
INSERT INTO src VALUES (2);
SELECT x FROM test_mv ORDER BY x;

-- 10. The REFRESH clause is covered by a dedicated test (`04227_create_or_replace_refresh_mv.sql`),
-- because refreshable MVs with non-replicated inner tables are refused on a Replicated database.

-- 11. OR REPLACE combined with IF NOT EXISTS is a syntax error (blocked at parser level)
CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS test_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src; -- { clientError SYNTAX_ERROR }

-- 12. Complex SELECT (JOIN) in AS clause parses and executes correctly
DROP TABLE IF EXISTS src2 SYNC;
CREATE TABLE src2 (x UInt32, label String) ENGINE = MergeTree ORDER BY x;
DROP TABLE IF EXISTS test_mv SYNC;
CREATE OR REPLACE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY x AS SELECT src.x, src2.label FROM src INNER JOIN src2 ON src.x = src2.x;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'test_mv';
DROP TABLE src2;

-- Cleanup
DROP TABLE IF EXISTS test_mv SYNC;
DROP TABLE IF EXISTS src SYNC;
DROP TABLE IF EXISTS target SYNC;
DROP TABLE IF EXISTS src2 SYNC;

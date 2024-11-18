-- Tests that Merge-engine (not: MergeTree!) tables support the trivial count
-- optimization if all underlying tables support it

DROP TABLE IF EXISTS mt1;
DROP TABLE IF EXISTS mt2;
DROP TABLE IF EXISTS merge;

CREATE TABLE mt1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mt2 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE merge (id UInt64) ENGINE = Merge(currentDatabase(), '^mt[0-9]+$');

INSERT INTO mt1 VALUES (1);
INSERT INTO mt2 VALUES (1);

SELECT count() FROM merge;

-- can use the trivial count optimization
EXPLAIN SELECT count() FROM merge settings enable_analyzer=0;

CREATE TABLE mt3 (id UInt64) ENGINE = TinyLog;

INSERT INTO mt2 VALUES (2);

SELECT count() FROM merge;

-- can't use the trivial count optimization as TinyLog doesn't support it
EXPLAIN SELECT count() FROM merge settings enable_analyzer=0;

DROP TABLE IF EXISTS mt1;
DROP TABLE IF EXISTS mt2;
DROP TABLE IF EXISTS mt3;
DROP TABLE IF EXISTS merge;

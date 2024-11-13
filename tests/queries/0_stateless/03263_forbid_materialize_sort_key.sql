CREATE TABLE IF NOT EXISTS test (a UInt64) ENGINE=MergeTree() ORDER BY a;

INSERT INTO test (a) SELECT 1 FROM numbers(1000);

ALTER TABLE test ADD COLUMN b Float64 AFTER a, MODIFY ORDER BY (a, b);
ALTER TABLE test MODIFY COLUMN b DEFAULT rand64() % 100000;
ALTER TABLE test MATERIALIZE COLUMN b;  -- { serverError CANNOT_UPDATE_COLUMN }


CREATE TABLE IF NOT EXISTS tab (x UInt32, y UInt32) engine = MergeTree ORDER BY tuple();
CREATE DICTIONARY IF NOT EXISTS dict (x UInt32, y UInt32) primary key x source(clickhouse(table 'tab')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
INSERT INTO tab values (1, 2), (3, 4);
SYSTEM RELOAD DICTIONARY dict;
CREATE TABLE IF NOT EXISTS tab2 (x UInt32, y UInt32 materialized dictGet(dict, 'y', x)) engine = MergeTree order by (y);
INSERT INTO tab2 (x) values (1), (3);
TRUNCATE TABLE tab;
INSERT INTO tab values (1, 4), (3, 2);
SYSTEM RELOAD DICTIONARY dict;
SET mutations_sync=2;
ALTER TABLE tab2 materialize column y;  -- { serverError CANNOT_UPDATE_COLUMN }

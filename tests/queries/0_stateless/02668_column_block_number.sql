DROP TABLE IF EXISTS test;

CREATE TABLE test (id UInt32, a UInt32) ENGINE = MergeTree ORDER BY id SETTINGS allow_experimental_block_number_column = true;

INSERT INTO test(id,a) VALUES (1,1),(2,2),(3,3);
INSERT INTO test(id,a) VALUES (4,4),(5,5),(6,6);

SELECT '*** BEFORE MUTATION BEFORE MERGE ***';
SELECT id,a,_block_number,_part from test ORDER BY id;

set mutations_sync=1;
ALTER TABLE test UPDATE a=0 WHERE id<4;

SELECT '*** AFTER MUTATION BEFORE MERGE ***';
SELECT id,a,_block_number,_part from test ORDER BY id;

OPTIMIZE TABLE test FINAL;

SELECT '*** AFTER MUTATION AFTER MERGE ***';
SELECT *,_block_number,_part from test ORDER BY id;

INSERT INTO test(id,a) VALUES (7,7),(8,8),(9,9);

SELECT '*** AFTER MUTATION AFTER MERGE , NEW BLOCK ***';
SELECT *,_block_number,_part from test ORDER BY id;

OPTIMIZE TABLE test FINAL;

SELECT '*** AFTER MUTATION AFTER MERGE , NEW BLOCK MERGED ***';
SELECT *,_block_number,_part from test ORDER BY id;

DROP TABLE test;
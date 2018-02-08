DROP TABLE IF EXISTS test.test;

CREATE TABLE IF NOT EXISTS test.test( id UInt32, track UInt8, codec String, content String, rdate Date DEFAULT '2018-02-03', track_id String DEFAULT concat(concat(concat(toString(track), '-'), codec), content) ) ENGINE=MergeTree(rdate, (id, track_id), 8192);

INSERT INTO test.test(id, track, codec) VALUES(1, 0, 'h264');

SELECT * FROM test.test ORDER BY id;

INSERT INTO test.test(id, track, codec, content) VALUES(2, 0, 'h264', 'CONTENT');

SELECT * FROM test.test ORDER BY id;

DROP TABLE IF EXISTS test.test;

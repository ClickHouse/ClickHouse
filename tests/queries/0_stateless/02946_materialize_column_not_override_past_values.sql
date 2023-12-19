
SET mutations_sync = 2;
-- Compact parts
CREATE TABLE test (id Int64, foo Int64 default 54321) ENGINE MergeTree ORDER BY id;
INSERT INTO test ( id, foo ) values ( 1, 2 );
INSERT INTO test ( id ) values ( 2 );
SELECT '--Origin--';
SELECT * FROM test ORDER BY id;
ALTER TABLE test MATERIALIZE COLUMN foo;
SELECT '--After materialize--';
SELECT * FROM test ORDER BY id;
DROP TABLE test;

-- Wide parts
CREATE TABLE test (id Int64, foo Nullable(Int64) default 54321) ENGINE MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO test ( id, foo ) values ( 1, 2 );
INSERT INTO test ( id ) values ( 2 );
SELECT '--Origin--';
SELECT * FROM test ORDER BY id;
ALTER TABLE test MATERIALIZE COLUMN foo;
SELECT '--After materialize--';
SELECT * FROM test ORDER BY id;
DROP TABLE test;

-- Nullable column != physically absent
CREATE TABLE test (id Int64, foo Nullable(Int64) default 54321) ENGINE MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1;
INSERT INTO test ( id, foo ) values ( 1, 2 );
INSERT INTO test ( id, foo ) values ( 2, NULL );
INSERT INTO test ( id ) values ( 3 );
SELECT '--Origin--';
SELECT * FROM test ORDER BY id;
ALTER TABLE test MATERIALIZE COLUMN foo;
SELECT '--After materialize--';
SELECT * FROM test ORDER BY id;
DROP TABLE test;

-- Parts with renamed column
CREATE TABLE test (id Int64, foo Int64 default 54321) ENGINE MergeTree ORDER BY id;
INSERT INTO test ( id, foo ) values ( 1, 2 );
INSERT INTO test ( id ) values ( 2 );
SELECT '--Origin--';
SELECT * FROM test ORDER BY id;
ALTER TABLE test RENAME COLUMN foo TO bar;
SELECT '--After rename--';
SELECT * FROM test ORDER BY id;
ALTER TABLE test MATERIALIZE COLUMN bar;
SELECT '--After materialize--';
SELECT * FROM test ORDER BY id;
DROP TABLE test;
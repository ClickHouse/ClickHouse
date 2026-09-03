-- Tags: no-fasttest

DROP TABLE IF EXISTS row_gate;

-- Persistent Row columns are gated behind an experimental setting.
SET allow_experimental_row_type = 0;
CREATE TABLE row_gate (a UInt64, r Row(x UInt64, y String)) ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE row_gate (a UInt64) ENGINE = MergeTree ORDER BY a;
ALTER TABLE row_gate ADD COLUMN r Row(x UInt64, y String); -- { serverError ILLEGAL_COLUMN }
SELECT * FROM format(TSV, 'r Row(x UInt64)', '(1)'); -- { serverError ILLEGAL_COLUMN }

SET allow_experimental_row_type = 1;
ALTER TABLE row_gate ADD COLUMN r Row(x UInt64, y String);
INSERT INTO row_gate VALUES (1, (1, 'b')), (2, (1, 'a')), (3, (0, 'z'));

-- Row is comparable like the named Tuple it mirrors.
SELECT toTypeName(r), r FROM row_gate ORDER BY r, a;
SELECT a, r = (1, 'a'), r < (1, 'b'), r > tuple(0, 'z'), r >= r FROM row_gate ORDER BY a;
SELECT count() FROM row_gate AS t1, row_gate AS t2 WHERE t1.r = t2.r;
SELECT (SELECT r FROM row_gate WHERE a = 1) = (SELECT r FROM row_gate WHERE a = 2);
SELECT r FROM row_gate WHERE r <= (1, 'a') ORDER BY r;

DROP TABLE row_gate;

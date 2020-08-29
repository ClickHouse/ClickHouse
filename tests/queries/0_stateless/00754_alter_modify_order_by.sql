SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS old_style;
CREATE TABLE old_style(d Date, x UInt32) ENGINE MergeTree(d, x, 8192);
ALTER TABLE old_style ADD COLUMN y UInt32, MODIFY ORDER BY (x, y); -- { serverError 36}
DROP TABLE old_style;

DROP TABLE IF EXISTS summing;
CREATE TABLE summing(x UInt32, y UInt32, val UInt32) ENGINE SummingMergeTree ORDER BY (x, y);

/* Can't add an expression with existing column to ORDER BY. */
ALTER TABLE summing MODIFY ORDER BY (x, y, -val); -- { serverError 36}

/* Can't add an expression with existing column to ORDER BY. */
ALTER TABLE summing ADD COLUMN z UInt32 DEFAULT x + 1, MODIFY ORDER BY (x, y, -z); -- { serverError 36}

/* Can't add nonexistent column to ORDER BY. */
ALTER TABLE summing MODIFY ORDER BY (x, y, nonexistent); -- { serverError 47}

/* Can't modyfy ORDER BY so that it is no longer a prefix of the PRIMARY KEY. */
ALTER TABLE summing MODIFY ORDER BY x; -- { serverError 36}

INSERT INTO summing(x, y, val) VALUES (1, 2, 10), (1, 2, 20);

ALTER TABLE summing ADD COLUMN z UInt32 AFTER y, MODIFY ORDER BY (x, y, -z);

INSERT INTO summing(x, y, z, val) values (1, 2, 1, 30), (1, 2, 2, 40), (1, 2, 2, 50);

SELECT '*** Check that the parts are sorted according to the new key. ***';
SELECT * FROM summing ORDER BY _part;

SELECT '*** Check that the rows are collapsed according to the new key. ***';
SELECT * FROM summing FINAL ORDER BY x, y, z;

SELECT '*** Check SHOW CREATE TABLE ***';
SHOW CREATE TABLE summing;

DROP TABLE summing;

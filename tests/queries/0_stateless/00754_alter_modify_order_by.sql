SET send_logs_level = 'fatal';
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS no_order;
CREATE TABLE no_order(a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE no_order MODIFY ORDER BY (a); -- { serverError BAD_ARGUMENTS}

DROP TABLE no_order;

DROP TABLE IF EXISTS old_style;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE old_style(d Date, x UInt32) ENGINE MergeTree(d, x, 8192);
ALTER TABLE old_style ADD COLUMN y UInt32, MODIFY ORDER BY (x, y); -- { serverError BAD_ARGUMENTS}
DROP TABLE old_style;

DROP TABLE IF EXISTS summing;
CREATE TABLE summing(x UInt32, y UInt32, val UInt32) ENGINE SummingMergeTree ORDER BY (x, y);

/* Can't add an expression with existing column to ORDER BY. */
ALTER TABLE summing MODIFY ORDER BY (x, y, -val); -- { serverError BAD_ARGUMENTS}

/* Can't add an expression with existing column to ORDER BY. */
ALTER TABLE summing ADD COLUMN z UInt32 DEFAULT x + 1, MODIFY ORDER BY (x, y, -z); -- { serverError BAD_ARGUMENTS}

/* Can't add nonexistent column to ORDER BY. */
ALTER TABLE summing MODIFY ORDER BY (x, y, nonexistent); -- { serverError UNKNOWN_IDENTIFIER}

/* Can't modyfy ORDER BY so that it is no longer a prefix of the PRIMARY KEY. */
ALTER TABLE summing MODIFY ORDER BY x; -- { serverError BAD_ARGUMENTS}

ALTER TABLE summing ADD COLUMN z UInt32 AFTER y, MODIFY ORDER BY (x, y, -z);

INSERT INTO summing(x, y, z, val) values (1, 2, 0, 10), (1, 2, 1, 30), (1, 2, 2, 40);

SELECT '*** Check that the parts are sorted according to the new key. ***';
SELECT * FROM summing;

INSERT INTO summing(x, y, z, val) values (1, 2, 0, 20), (1, 2, 2, 50);

SELECT '*** Check that the rows are collapsed according to the new key. ***';
SELECT * FROM summing FINAL ORDER BY x, y, z;

SELECT '*** Check SHOW CREATE TABLE ***';
SHOW CREATE TABLE summing;

DROP TABLE summing;

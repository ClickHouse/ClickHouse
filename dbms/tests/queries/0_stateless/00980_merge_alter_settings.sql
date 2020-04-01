DROP TABLE IF EXISTS log_for_alter;

CREATE TABLE log_for_alter (
  id UInt64,
  Data String
) ENGINE = Log();

ALTER TABLE log_for_alter MODIFY SETTING aaa=123; -- { serverError 36 }

DROP TABLE IF EXISTS log_for_alter;

DROP TABLE IF EXISTS table_for_alter;

CREATE TABLE table_for_alter (
  id UInt64,
  Data String
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity=4096;

ALTER TABLE table_for_alter MODIFY SETTING index_granularity=555; -- { serverError 472 }

SHOW CREATE TABLE table_for_alter;

ALTER TABLE table_for_alter MODIFY SETTING  parts_to_throw_insert = 1, parts_to_delay_insert = 1;

SHOW CREATE TABLE table_for_alter;

INSERT INTO table_for_alter VALUES (1, '1');
INSERT INTO table_for_alter VALUES (2, '2'); -- { serverError 252 }

DETACH TABLE table_for_alter;

ATTACH TABLE table_for_alter;

INSERT INTO table_for_alter VALUES (2, '2'); -- { serverError 252 }

ALTER TABLE table_for_alter MODIFY SETTING xxx_yyy=124; -- { serverError 115 }

ALTER TABLE table_for_alter MODIFY SETTING parts_to_throw_insert = 100, parts_to_delay_insert = 100;

INSERT INTO table_for_alter VALUES (2, '2');

SHOW CREATE TABLE table_for_alter;

SELECT COUNT() FROM table_for_alter;

ALTER TABLE table_for_alter MODIFY SETTING check_delay_period=10, check_delay_period=20, check_delay_period=30;

SHOW CREATE TABLE table_for_alter;

ALTER TABLE table_for_alter ADD COLUMN Data2 UInt64, MODIFY SETTING check_delay_period=5, check_delay_period=10, check_delay_period=15;

SHOW CREATE TABLE table_for_alter;

DROP TABLE IF EXISTS table_for_alter;


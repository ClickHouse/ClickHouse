DROP TABLE IF EXISTS table_for_alter;

CREATE TABLE table_for_alter (
  id UInt64,
  Data String
) ENGINE = MergeTree() ORDER BY id SETTINGS parts_to_throw_insert = 1, parts_to_delay_insert = 1;

INSERT INTO table_for_alter VALUES (1, '1');
INSERT INTO table_for_alter VALUES (2, '2'); -- { serverError TOO_MANY_PARTS }

INSERT INTO table_for_alter settings parts_to_throw_insert = 100, parts_to_delay_insert = 100 VALUES (2, '2');

INSERT INTO table_for_alter VALUES (3, '3'); -- { serverError TOO_MANY_PARTS }

ALTER TABLE table_for_alter MODIFY SETTING parts_to_throw_insert = 100, parts_to_delay_insert = 100;

INSERT INTO table_for_alter VALUES (3, '3');

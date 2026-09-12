-- Disable force_primary_key_reverse_order: SHOW CREATE output contains ORDER BY which changes with forced DESC
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS alter_test;
CREATE TABLE alter_test (a Int32, b DateTime) ENGINE = ReplacingMergeTree(b) ORDER BY a;
ALTER TABLE alter_test MODIFY COLUMN `b` DateTime DEFAULT now();
ALTER TABLE alter_test MODIFY COLUMN `b` DEFAULT now() + 1;
SHOW CREATE TABLE alter_test;
DROP TABLE alter_test;

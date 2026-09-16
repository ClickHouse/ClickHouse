-- Test the `table_readonly` MergeTree setting.

DROP TABLE IF EXISTS t_readonly;

CREATE TABLE t_readonly (x UInt64) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_readonly VALUES (1);
SELECT count() FROM t_readonly;

-- Mark the table as readonly.
ALTER TABLE t_readonly MODIFY SETTING table_readonly = 1;

-- Inserts should fail.
INSERT INTO t_readonly VALUES (2); -- { serverError TABLE_IS_READ_ONLY }

-- Mutations should fail.
ALTER TABLE t_readonly DELETE WHERE x = 1; -- { serverError TABLE_IS_READ_ONLY }

-- But changing settings should still work (so we can toggle readonly back).
ALTER TABLE t_readonly MODIFY SETTING table_readonly = 0;

-- Now inserts should work again.
INSERT INTO t_readonly VALUES (3);
SELECT count() FROM t_readonly;

DROP TABLE t_readonly;

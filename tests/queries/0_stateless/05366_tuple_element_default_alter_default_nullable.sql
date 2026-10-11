-- With `data_type_default_nullable`, `ALTER TABLE ... ADD COLUMN` / `MODIFY COLUMN` must pull tuple-element
-- defaults up to the column level before the nullable rewrite builds the data type.

DROP TABLE IF EXISTS t_tuple_default_nullable;
CREATE TABLE t_tuple_default_nullable (id UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_tuple_default_nullable VALUES (1);

SET data_type_default_nullable = 1;

ALTER TABLE t_tuple_default_nullable ADD COLUMN c Tuple(a UInt8 DEFAULT 1, s String DEFAULT 'Hello');
SELECT id, c FROM t_tuple_default_nullable ORDER BY id;

ALTER TABLE t_tuple_default_nullable MODIFY COLUMN c Tuple(a UInt8 DEFAULT 2, s String DEFAULT 'World');
INSERT INTO t_tuple_default_nullable (id) VALUES (2);
SELECT id, c FROM t_tuple_default_nullable ORDER BY id;

SELECT name, type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_default_nullable' ORDER BY position;

-- The unsupported `Nested` case reports `NOT_IMPLEMENTED` on this path too.
ALTER TABLE t_tuple_default_nullable ADD COLUMN n Nested(x UInt8 DEFAULT 1); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_tuple_default_nullable;

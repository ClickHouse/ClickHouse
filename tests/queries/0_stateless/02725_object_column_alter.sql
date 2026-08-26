-- Eventually this ALTER should be fixed

SET allow_experimental_object_type=1;

DROP TABLE IF EXISTS t_to;
CREATE TABLE t_to (id UInt64, value Nullable(String)) ENGINE MergeTree() ORDER BY id;
ALTER TABLE t_to MODIFY COLUMN value Object('json'); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_to;


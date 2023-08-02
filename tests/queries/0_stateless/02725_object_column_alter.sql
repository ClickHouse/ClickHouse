-- Eventually this ALTER should be fixed

SET allow_experimental_object_type=1;

DROP TABLE IF EXISTS t_to;
DROP TABLE IF EXISTS t_from;

CREATE TABLE t_to (id UInt64, value Nullable(String)) ENGINE MergeTree() ORDER BY id;
CREATE TABLE t_from (id UInt64, value Object('json')) ENGINE MergeTree() ORDER BY id;

ALTER TABLE t_to MODIFY COLUMN value Object('json'); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_from MODIFY COLUMN value Nullable(String); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_to;
DROP TABLE t_from;

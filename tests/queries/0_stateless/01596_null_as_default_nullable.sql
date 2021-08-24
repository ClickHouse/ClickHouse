-- Check that "null as default" applies only if type is not Nullable.

SET input_format_null_as_default = 1;
CREATE TEMPORARY TABLE t (x Nullable(String) DEFAULT 'Hello', y String DEFAULT 'World');
INSERT INTO t VALUES (NULL, NULL);
SELECT * FROM t;

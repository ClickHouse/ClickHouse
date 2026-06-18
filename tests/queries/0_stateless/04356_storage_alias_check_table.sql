SET allow_experimental_alias_table_engine = 1;

CREATE TABLE t_base (id UInt32) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_alias ENGINE = Alias('t_base');
INSERT INTO t_base VALUES (1), (2), (3);

-- CHECK TABLE on alias delegates to target
SET check_query_single_value_result = 1;
CHECK TABLE t_alias;

DROP TABLE t_alias;
DROP TABLE t_base;

-- A table created from a table function keeps its comment: the comment is not passed to the
-- table function (unlike to a table engine), so it has to be applied to the storage explicitly,
-- both when the table is created and when its definition is loaded back from the metadata.

DROP TABLE IF EXISTS t_comment_table_function;

CREATE TABLE t_comment_table_function (dummy UInt8) AS merge('system', '^one$') COMMENT 'Union of one';

SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't_comment_table_function';

DETACH TABLE t_comment_table_function;
ATTACH TABLE t_comment_table_function;

SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't_comment_table_function';

DROP TABLE t_comment_table_function;

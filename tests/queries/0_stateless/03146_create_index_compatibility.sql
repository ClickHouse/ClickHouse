DROP TABLE IF EXISTS t_index_3146;

CREATE TABLE t_index_3146 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();

SET allow_create_index_without_type = 1;

CREATE INDEX i1 ON t_index_3146 (a) TYPE minmax;
CREATE INDEX i2 ON t_index_3146 (a, b) TYPE minmax;
CREATE INDEX i3 ON t_index_3146 (a DESC, b ASC) TYPE minmax;
CREATE INDEX i4 ON t_index_3146 a TYPE minmax;
CREATE INDEX i5 ON t_index_3146 (a); -- ignored
CREATE INDEX i6 ON t_index_3146 (a DESC, b ASC); -- ignored
CREATE INDEX i7 ON t_index_3146; -- { clientError SYNTAX_ERROR }
CREATE INDEX i8 ON t_index_3146 a, b TYPE minmax; -- { clientError SYNTAX_ERROR }

SHOW CREATE TABLE t_index_3146;
DROP TABLE t_index_3146;

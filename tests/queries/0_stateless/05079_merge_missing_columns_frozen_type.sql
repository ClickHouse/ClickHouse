-- With `skip_empty_columns_on_insert`, a part whose column holds only default values is written without
-- the column and records the column's type frozen at write time. Two parts can disagree on that type
-- after an `ALTER ... MODIFY COLUMN` that did not rewrite them; the merged part keeps a single marker per
-- column, so the rows of the part whose marker was dropped used to read as the other part's default.

DROP TABLE IF EXISTS t_missing_columns_frozen_type;
CREATE TABLE t_missing_columns_frozen_type (k UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';

INSERT INTO t_missing_columns_frozen_type (k, b) VALUES (1, 0);
ALTER TABLE t_missing_columns_frozen_type DETACH PARTITION tuple();
ALTER TABLE t_missing_columns_frozen_type MODIFY COLUMN b String;
ALTER TABLE t_missing_columns_frozen_type ATTACH PARTITION tuple();
INSERT INTO t_missing_columns_frozen_type (k, b) VALUES (2, '');

SELECT k, b, length(b) FROM t_missing_columns_frozen_type ORDER BY k;
OPTIMIZE TABLE t_missing_columns_frozen_type FINAL;
SELECT k, b, length(b) FROM t_missing_columns_frozen_type ORDER BY k;

DROP TABLE t_missing_columns_frozen_type;

-- Parts that agree on the frozen type keep the marker, so the merge still writes no data for the column.
DROP TABLE IF EXISTS t_missing_columns_same_type;
CREATE TABLE t_missing_columns_same_type (k UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';

INSERT INTO t_missing_columns_same_type (k, b) VALUES (1, 0);
INSERT INTO t_missing_columns_same_type (k, b) VALUES (2, 0);
OPTIMIZE TABLE t_missing_columns_same_type FINAL;

SELECT k, b FROM t_missing_columns_same_type ORDER BY k;

DROP TABLE t_missing_columns_same_type;

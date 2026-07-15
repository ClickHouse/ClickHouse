DROP TABLE IF EXISTS t_null_sub_evolved;

-- A part written before a metadata-only `T` -> `Nullable(T)` ALTER stores non-nullable
-- data and has no `.null` substream. Reading the `.null` subcolumn (as done by
-- optimize_functions_to_subcolumns for isNull/isNotNull/count) must derive it from the
-- present parent column, giving null-map = 0 for the existing rows, not the storage-type
-- default (NULL).

CREATE TABLE t_null_sub_evolved (id UInt8, x UInt256)
ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 42);

ALTER TABLE t_null_sub_evolved
    MODIFY COLUMN x Nullable(UInt256) DEFAULT NULL
SETTINGS mutations_sync = 2;

SELECT 'compact, DEFAULT NULL';
SELECT count() FROM t_null_sub_evolved WHERE id = 1 AND x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE id = 1 AND x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

-- No DEFAULT, add a genuine NULL in a new part; check isNull/isNotNull/count.
CREATE TABLE t_null_sub_evolved (id UInt8, x UInt256)
ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 42);
ALTER TABLE t_null_sub_evolved MODIFY COLUMN x Nullable(UInt256) SETTINGS mutations_sync = 2;
INSERT INTO t_null_sub_evolved VALUES (2, NULL);

SELECT 'compact, no default';
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_null_sub_evolved WHERE x IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(x) FROM t_null_sub_evolved SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(x) FROM t_null_sub_evolved SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

-- Wide part.
CREATE TABLE t_null_sub_evolved (id UInt8, x UInt256)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 42);
ALTER TABLE t_null_sub_evolved MODIFY COLUMN x Nullable(UInt256) SETTINGS mutations_sync = 2;

SELECT 'wide';
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

-- String -> Nullable(String) evolution.
CREATE TABLE t_null_sub_evolved (id UInt8, x String)
ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 'a');
ALTER TABLE t_null_sub_evolved MODIFY COLUMN x Nullable(String) SETTINGS mutations_sync = 2;
INSERT INTO t_null_sub_evolved VALUES (2, NULL);

SELECT 'string';
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

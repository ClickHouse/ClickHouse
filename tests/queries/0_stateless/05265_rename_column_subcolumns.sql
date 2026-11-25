-- Renaming a column renames its subcolumns: the old subcolumn names must not stay resolvable,
-- neither through a Buffer table that still has the old names nor after a new column reuses an old name.

-- The Buffer table reports every column missing in its destination with a warning.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_rename_subcolumns_buffer;
DROP TABLE IF EXISTS t_rename_subcolumns;

CREATE TABLE t_rename_subcolumns (c0 Nullable(UInt8), a Array(UInt8)) ENGINE = Memory;
CREATE TABLE t_rename_subcolumns_buffer (c0 Nullable(UInt8), a Array(UInt8))
    ENGINE = Buffer(currentDatabase(), t_rename_subcolumns, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_rename_subcolumns_buffer VALUES (5, [1, 2]), (NULL, []);

ALTER TABLE t_rename_subcolumns RENAME COLUMN c0 TO c1, RENAME COLUMN a TO b;

SELECT c0, c0.null, a.size0 FROM t_rename_subcolumns_buffer ORDER BY c0.null;
DROP TABLE t_rename_subcolumns_buffer;

INSERT INTO t_rename_subcolumns VALUES (7, [1, 2, 3]);
SELECT c1, c1.null, b.size0 FROM t_rename_subcolumns;

ALTER TABLE t_rename_subcolumns ADD COLUMN c0 UInt64, ADD COLUMN a UInt64;
SELECT c0.null FROM t_rename_subcolumns; -- { serverError UNKNOWN_IDENTIFIER }
SELECT a.size0 FROM t_rename_subcolumns; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE t_rename_subcolumns;

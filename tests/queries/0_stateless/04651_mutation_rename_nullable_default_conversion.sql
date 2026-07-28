-- A part that has not applied a `RENAME COLUMN` yet still stores the column under its old name,
-- while the mutation's required columns use the current metadata name. Resolving the on-part name
-- through the alter conversions is what lets the nullable-to-non-null default conversion pull in
-- the columns its default expression reads.

DROP TABLE IF EXISTS t_rename_nullable_default;
CREATE TABLE t_rename_nullable_default
(
    a Nullable(UInt64),
    src UInt64,
    v UInt64,
    k UInt64
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_rename_nullable_default VALUES (NULL, 7, 0, 1), (10, 8, 0, 2);

SYSTEM STOP MERGES t_rename_nullable_default;

-- Metadata-only rename: the part keeps `a`, the metadata knows `b`.
ALTER TABLE t_rename_nullable_default RENAME COLUMN a TO b SETTINGS mutations_sync = 0, alter_sync = 0;

-- The default is written with a column matcher, so it is expanded before the conversion is analyzed.
ALTER TABLE t_rename_nullable_default MODIFY COLUMN b UInt64 DEFAULT greatest(COLUMNS('^src$')) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT b, src FROM t_rename_nullable_default ORDER BY k;

-- A mutation over the still-renamed part has to read `src` to materialize `b`.
SYSTEM START MERGES t_rename_nullable_default;
ALTER TABLE t_rename_nullable_default UPDATE v = b WHERE k = 1 SETTINGS mutations_sync = 2;

SELECT b, src, v FROM t_rename_nullable_default ORDER BY k;

DROP TABLE t_rename_nullable_default;

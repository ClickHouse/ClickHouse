-- A part that predates a `MODIFY COLUMN` from `Nullable` to a non-nullable type with a `DEFAULT`
-- still stores nullable data, so reads and mutations over it have to pull in the columns the
-- default expression reads to convert `NULL` values. The default is written with a column
-- matcher, so it is expanded before the conversion is analyzed.
--
-- Since the `RENAME COLUMN` mutation became a hard barrier for subsequent alters (any later
-- `ALTER` waits for it regardless of `alter_sync`), the rename is waited for synchronously here
-- and only the type-conversion mutation is kept pending with stopped merges.

DROP TABLE IF EXISTS t_rename_nullable_default;
CREATE TABLE t_rename_nullable_default
(
    a Nullable(UInt64),
    src UInt64,
    v UInt64,
    k UInt64
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_rename_nullable_default VALUES (NULL, 7, 0, 1), (10, 8, 0, 2);

ALTER TABLE t_rename_nullable_default RENAME COLUMN a TO b;

-- Keep the type-conversion mutation pending, so the part still stores nullable data.
SYSTEM STOP MERGES t_rename_nullable_default;

ALTER TABLE t_rename_nullable_default MODIFY COLUMN b UInt64 DEFAULT greatest(COLUMNS('^src$')) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT b, src FROM t_rename_nullable_default ORDER BY k;

-- A mutation over the still-nullable part has to read `src` to materialize `b`.
SYSTEM START MERGES t_rename_nullable_default;
ALTER TABLE t_rename_nullable_default UPDATE v = b WHERE k = 1 SETTINGS mutations_sync = 2;

SELECT b, src, v FROM t_rename_nullable_default ORDER BY k;

DROP TABLE t_rename_nullable_default;

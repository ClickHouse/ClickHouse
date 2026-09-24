-- { echo ON }

DROP TABLE IF EXISTS t_projection_codecs_alter;

CREATE TABLE t_projection_codecs_alter (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0;

-- `ADD PROJECTION` goes through the same declaration parser as `CREATE TABLE`.
ALTER TABLE t_projection_codecs_alter
    ADD PROJECTION p (x UInt64 CODEC(NONE), y UInt64) AS (SELECT x, y ORDER BY x);

INSERT INTO t_projection_codecs_alter SELECT number, number FROM numbers(100000);

-- Same size check as `04846_projection_column_codecs`, on the `ALTER` path.
SELECT column, column_data_compressed_bytes > column_data_uncompressed_bytes AS is_uncompressed
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_alter' AND active AND name = 'p'
    AND column IN ('x', 'y')
ORDER BY column;

-- The column list must survive into the stored table definition.
SHOW CREATE TABLE t_projection_codecs_alter;

DETACH TABLE t_projection_codecs_alter;
ATTACH TABLE t_projection_codecs_alter;
SHOW CREATE TABLE t_projection_codecs_alter;

-- `AlterCommands` reports which projection broke rather than silently re-deriving it.
ALTER TABLE t_projection_codecs_alter MODIFY COLUMN x String; -- { serverError TYPE_MISMATCH }

ALTER TABLE t_projection_codecs_alter DROP PROJECTION p;
SHOW CREATE TABLE t_projection_codecs_alter;

DROP TABLE t_projection_codecs_alter;

-- The same validation applies on the `ALTER` path as on `CREATE`.
CREATE TABLE t_projection_codecs_alter (x UInt64)
ENGINE = MergeTree ORDER BY x;

ALTER TABLE t_projection_codecs_alter
    ADD PROJECTION p (z UInt64 CODEC(NONE)) AS (SELECT x ORDER BY x); -- { serverError THERE_IS_NO_COLUMN }

ALTER TABLE t_projection_codecs_alter
    ADD PROJECTION p (x String CODEC(NONE)) AS (SELECT x ORDER BY x); -- { serverError TYPE_MISMATCH }

ALTER TABLE t_projection_codecs_alter
    ADD PROJECTION p (x UInt64 COMMENT 'c') AS (SELECT x ORDER BY x); -- { serverError NOT_IMPLEMENTED }

-- `AlterCommands::validate` runs with the user's context, so the setting applies here too.
ALTER TABLE t_projection_codecs_alter
    ADD PROJECTION p (x UInt64 CODEC(Delta, Delta)) AS (SELECT x ORDER BY x); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_codecs = 1;
ALTER TABLE t_projection_codecs_alter
    ADD PROJECTION p (x UInt64 CODEC(Delta, Delta)) AS (SELECT x ORDER BY x);

-- Building the metadata must not re-check the codec, or the table would stop loading once the setting
-- went back to its default. Reset first, so the round trip is not simply allowed a second time.
SET allow_suspicious_codecs = 0;
DETACH TABLE t_projection_codecs_alter;
ATTACH TABLE t_projection_codecs_alter;
SELECT name, codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codecs_alter';

DROP TABLE t_projection_codecs_alter;

-- The same untyped-declaration cases on the `ALTER` path, which runs its own validation pass.
CREATE TABLE t_alter_untyped (k UInt64, ts DateTime, id UInt64)
ENGINE = MergeTree ORDER BY k;

ALTER TABLE t_alter_untyped
    ADD PROJECTION p (k DEFAULT 1 CODEC(NONE)) AS (SELECT k ORDER BY k); -- { serverError NOT_IMPLEMENTED }

ALTER TABLE t_alter_untyped
    ADD PROJECTION p (k MATERIALIZED 1 CODEC(NONE)) AS (SELECT k ORDER BY k); -- { serverError NOT_IMPLEMENTED }

ALTER TABLE t_alter_untyped
    ADD PROJECTION p (k ALIAS 1 CODEC(NONE)) AS (SELECT k ORDER BY k); -- { serverError NOT_IMPLEMENTED }

ALTER TABLE t_alter_untyped
    ADD PROJECTION p (k EPHEMERAL 1 CODEC(NONE)) AS (SELECT k ORDER BY k); -- { serverError NOT_IMPLEMENTED }

-- Omitting the type keeps the column free to change with the parent table; declaring it pins the
-- column for as long as the projection exists. Both halves are asserted below.
ALTER TABLE t_alter_untyped
    ADD PROJECTION p (ts CODEC(DoubleDelta), id UInt64 CODEC(NONE)) AS (SELECT k, ts, id ORDER BY k);

SHOW CREATE TABLE t_alter_untyped;

-- `ts` was declared without a type, so widening it is allowed.
ALTER TABLE t_alter_untyped MODIFY COLUMN ts DateTime64(3);

-- `id` was declared as `UInt64`, so changing it breaks the declaration.
ALTER TABLE t_alter_untyped MODIFY COLUMN id Int64; -- { serverError TYPE_MISMATCH }

-- The declaration survives the widening, codec and all.
SHOW CREATE TABLE t_alter_untyped;
SELECT name, codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_alter_untyped';

INSERT INTO t_alter_untyped SELECT number, toDateTime64(number, 3), number FROM numbers(1000);
SELECT count() FROM t_alter_untyped;

DROP TABLE t_alter_untyped;

-- Validation follows statement order: the projection sees the Float64 type installed by the
-- preceding command, so Gorilla is not judged against the old UInt64 type.
CREATE TABLE t_alter_ordered (x UInt64) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE t_alter_ordered
    MODIFY COLUMN x Float64,
    ADD PROJECTION p (x CODEC(Gorilla)) AS (SELECT x ORDER BY x);

SELECT name, codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_alter_ordered';

DROP TABLE t_alter_ordered;

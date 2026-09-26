-- ALTER TABLE ... RECOMPRESS COLUMN must reject non-physical targets.
--
-- `RECOMPRESS COLUMN` is parsed straight into a `MutationCommand` and never goes through the
-- `AlterCommands` validation, so a bad target used to slip through to the per-part mutation path:
-- an unknown / `ALIAS` / `EPHEMERAL` name has no on-disk stream, so on wide parts the ALTER silently
-- did nothing, while on compact parts it fell through to a whole-part rewrite of unrelated columns.
-- `MergeTreeData::checkMutationIsPossible` now rejects such targets up front (before per-part
-- dispatch): NO_SUCH_COLUMN_IN_TABLE = 16 for an unknown name, BAD_ARGUMENTS = 36 for ALIAS/EPHEMERAL.

DROP TABLE IF EXISTS recompress_non_physical;
CREATE TABLE recompress_non_physical
(
    a UInt32 CODEC(NONE),
    al UInt32 ALIAS a + 1,
    ep UInt32 EPHEMERAL 0
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO recompress_non_physical (a) VALUES (1), (2);

SELECT 'unknown_column';
ALTER TABLE recompress_non_physical RECOMPRESS COLUMN nonexistent; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT 'alias_column';
ALTER TABLE recompress_non_physical RECOMPRESS COLUMN al; -- { serverError BAD_ARGUMENTS }

SELECT 'ephemeral_column';
ALTER TABLE recompress_non_physical RECOMPRESS COLUMN ep; -- { serverError BAD_ARGUMENTS }

-- Negative control: a physical column is accepted.
SET mutations_sync = 2;
SELECT 'physical_column';
ALTER TABLE recompress_non_physical MODIFY COLUMN a CODEC(ZSTD);
ALTER TABLE recompress_non_physical RECOMPRESS COLUMN a;

SELECT count() FROM recompress_non_physical;  -- 2

DROP TABLE recompress_non_physical;

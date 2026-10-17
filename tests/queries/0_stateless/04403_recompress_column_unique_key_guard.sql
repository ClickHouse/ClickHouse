-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- UNIQUE KEY: ALTER TABLE ... RECOMPRESS COLUMN must be rejected.
--
-- `RECOMPRESS COLUMN` of a Compact part (or of a column that inherits the table's default codec) goes
-- through the whole-part rewrite path, which does not preserve the `delete_bitmap_*.rbm` sidecars of a
-- UNIQUE KEY table and would resurrect deleted rows. `MergeTreeData::checkMutationIsPossible` rejects it
-- universally (before per-part dispatch) with SUPPORT_IS_DISABLED = 344.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_recompress_guard;
CREATE TABLE uk_recompress_guard (a UInt32, b UInt32, s String CODEC(NONE))
ENGINE = MergeTree ORDER BY (a) UNIQUE KEY (a, b);

INSERT INTO uk_recompress_guard VALUES (1, 10, 'x'), (2, 20, 'y');

ALTER TABLE uk_recompress_guard MODIFY COLUMN s CODEC(ZSTD);

-- Rejected regardless of whether the column has an explicit codec (`s`) or inherits the default (`b`).
SELECT 'recompress_uk_explicit_codec' AS step;
ALTER TABLE uk_recompress_guard RECOMPRESS COLUMN s; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'recompress_uk_inherited_codec' AS step;
ALTER TABLE uk_recompress_guard RECOMPRESS COLUMN b; -- { serverError SUPPORT_IS_DISABLED }

-- The table is untouched: both rows still present.
SELECT count() FROM uk_recompress_guard;  -- 2

DROP TABLE uk_recompress_guard;

-- Negative control: RECOMPRESS COLUMN succeeds on a plain (non-UNIQUE-KEY) MergeTree table.
SET mutations_sync = 2;
DROP TABLE IF EXISTS mt_recompress_ok;
CREATE TABLE mt_recompress_ok (a UInt32, s String CODEC(NONE))
ENGINE = MergeTree ORDER BY a;

INSERT INTO mt_recompress_ok VALUES (1, 'x'), (2, 'y');

ALTER TABLE mt_recompress_ok MODIFY COLUMN s CODEC(ZSTD);
ALTER TABLE mt_recompress_ok RECOMPRESS COLUMN s;

SELECT count() FROM mt_recompress_ok;  -- 2

DROP TABLE mt_recompress_ok;

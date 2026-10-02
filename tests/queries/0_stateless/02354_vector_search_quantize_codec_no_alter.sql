-- The `Quantized(...)` codec is immutable via ALTER: it can only be set at CREATE TABLE. Adding, removing, or changing
-- it on an existing column via ALTER is rejected, because a codec change is metadata-only (existing parts are not
-- rewritten) and would leave the table inconsistent - old parts without the companion codes stream, new parts with it.
-- To adopt it on existing data, recreate the table with the codec and INSERT ... SELECT into it.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_alter;

-- Setting the codec at CREATE TABLE works (the only way in).
CREATE TABLE quantize_alter (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY id;

-- Changing the codec parameters, switching the method, or removing the codec (explicitly, via CODEC(NONE)) is rejected.
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(Quantized('rabitq', 128)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(Quantized('turboquant', 64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(NONE); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Any MODIFY COLUMN that restates the type is rejected, because it reaches ColumnsDescription::modify and reassigns the
-- column type without reattaching the codec's custom serialization - even when the type is textually unchanged. This
-- covers a bare same-type restatement, a same-type restatement carrying only a COMMENT, and a genuine type change.
-- Otherwise the metadata would still say CODEC(Quantized(...)) while new writes stop producing the companion codes.
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) COMMENT 'x'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float64) CODEC(Quantized('rabitq', 64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A comment-only MODIFY COLUMN (no type clause) preserves the serialization and is allowed.
ALTER TABLE quantize_alter MODIFY COLUMN vec COMMENT 'kept';
SELECT 'comment_kept', comment FROM system.columns WHERE database = currentDatabase() AND table = 'quantize_alter' AND name = 'vec';

DROP TABLE quantize_alter;

-- Adding the codec to a plain existing column, or adding a new column that carries it, is rejected.
CREATE TABLE quantize_alter (id UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(Quantized('rabitq', 64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter ADD COLUMN vec2 Array(Float32) CODEC(Quantized('rabitq', 64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE quantize_alter;

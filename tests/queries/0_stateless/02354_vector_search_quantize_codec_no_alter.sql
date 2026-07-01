-- The `Quantize(...)` codec is immutable via ALTER: it can only be set at CREATE TABLE. Adding, removing, or changing
-- it on an existing column via ALTER is rejected, because a codec change is metadata-only (existing parts are not
-- rewritten) and would leave the table inconsistent - old parts without the companion codes stream, new parts with it.
-- To adopt it on existing data, recreate the table with the codec and INSERT ... SELECT into it.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_alter;

-- Setting the codec at CREATE TABLE works (the only way in).
CREATE TABLE quantize_alter (id UInt32, vec Array(Float32) CODEC(Quantize('rabitq', 64))) ENGINE = MergeTree ORDER BY id;

-- Changing the codec parameters, switching the method, or removing the codec (explicitly, via CODEC(NONE)) is rejected.
-- (A bare MODIFY COLUMN vec Array(Float32) without a CODEC clause keeps the existing codec, so it is a no-op here.)
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(Quantize('rabitq', 128)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(Quantize('turboquant', 64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(NONE); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- An ALTER that leaves the codec unchanged is allowed (a comment-only modify must not be mistaken for removing it).
ALTER TABLE quantize_alter MODIFY COLUMN vec COMMENT 'kept';
SELECT 'comment_kept', comment FROM system.columns WHERE database = currentDatabase() AND table = 'quantize_alter' AND name = 'vec';

DROP TABLE quantize_alter;

-- Adding the codec to a plain existing column, or adding a new column that carries it, is rejected.
CREATE TABLE quantize_alter (id UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE quantize_alter MODIFY COLUMN vec Array(Float32) CODEC(Quantize('rabitq', 64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE quantize_alter ADD COLUMN vec2 Array(Float32) CODEC(Quantize('rabitq', 64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE quantize_alter;

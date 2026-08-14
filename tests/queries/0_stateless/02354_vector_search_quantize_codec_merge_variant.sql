-- A column with a `Quantized(...)` codec attaches the SerializationQuantizedVector custom serialization
-- directly (not through the serialization pool), so it must not report supportsPooling()==true. Otherwise,
-- when such a column is nested inside a poolable parent serialization (e.g. a Variant assembled by a Merge
-- table over sources with differing types), getHash() fires "Hash is not set for serialization".

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS qv_merge_a;
DROP TABLE IF EXISTS qv_merge_b;

CREATE TABLE qv_merge_a (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE qv_merge_b (id UInt32, vec String) ENGINE = MergeTree ORDER BY id;

-- The differing `vec` types make StorageMerge merge the column into a Variant, whose default serialization
-- hashes each child. Before the fix this aborts in getHash(); after the fix it succeeds.
SELECT count() FROM merge(currentDatabase(), '^qv_merge_[ab]$');

DROP TABLE qv_merge_a;
DROP TABLE qv_merge_b;

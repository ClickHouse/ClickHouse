-- Tags: long

SET allow_experimental_json_type = 1;

-- Regression test for a bug where ColumnObject::index (and filter/replicate/scatter)
-- did not propagate statistics, causing a mismatch between the number of shared data
-- buckets chosen during stream creation vs serialization state creation for nested JSON
-- columns inside Array(JSON). This resulted in:
-- "Stream ... object_shared_data.1.size1 not found" (LOGICAL_ERROR)
--
-- The bug requires: Wide parts, nested JSON with empty shared data, a non-trivial
-- permutation applied during INSERT, and optimize_on_insert=0 to prevent mergeBlock
-- from pre-sorting the block and nullifying the permutation.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (id UInt64, data JSON(max_dynamic_paths=256))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part=0;

INSERT INTO src VALUES
    (3, '{"images": [{"url": "a", "width": 100}]}'),
    (2, '{"images": [{"url": "d", "width": 400}]}'),
    (1, '{"images": [{"url": "c", "width": 300}]}');

CREATE TABLE dst (id UInt64, data JSON(max_dynamic_paths=256))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0;

-- Data arrives at the MergeTree sink with statistics intact from reading the source part.
-- The sink applies a permutation to sort by id. With optimize_on_insert=0, the raw
-- permutation is passed to the writer. The inner JSON (inside Array) goes through
-- ColumnArray::permute -> ColumnArray::indexImpl -> ColumnObject::index.
-- Before the fix, ColumnObject::index dropped statistics, causing a bucket count mismatch.
INSERT INTO dst SELECT * FROM src
SETTINGS max_insert_threads=1, optimize_on_insert=0;

SELECT id, data FROM dst ORDER BY id;

DROP TABLE src;
DROP TABLE dst;

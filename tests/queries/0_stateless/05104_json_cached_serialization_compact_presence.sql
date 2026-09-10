DROP TABLE IF EXISTS json_cached_compact_presence;

CREATE TABLE json_cached_compact_presence
(
    id UInt64,
    json JSON(max_dynamic_paths = 0, typed UInt64, arr Array(UInt64)),
    wrapped Tuple(a JSON, `a.b` UInt32)
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = '100G', write_marks_for_substreams_in_compact_parts = 0;

INSERT INTO json_cached_compact_presence VALUES (1, '{"typed":11,"arr":[1,2],"dynamic":31}', ('{"b":999,"c":41}', 51));

ALTER TABLE json_cached_compact_presence MODIFY SETTING write_marks_for_substreams_in_compact_parts = 1;
INSERT INTO json_cached_compact_presence VALUES (2, '{"typed":12,"arr":[3],"dynamic":32}', ('{"b":999,"c":42}', 52));

SELECT part_type, count()
FROM system.parts
WHERE database = currentDatabase() AND table = 'json_cached_compact_presence' AND active
GROUP BY part_type;

-- Read typed, derived, dynamic, missing, and colliding subcolumn names from both mark formats.
SELECT id, json.typed, json.arr.size0, json.dynamic.:Int64, json.missing.:Int64, wrapped.`a.b`, wrapped.a.c.:Int64
FROM json_cached_compact_presence ORDER BY id;

-- The part's original column name must still be used after a metadata-only rename.
ALTER TABLE json_cached_compact_presence RENAME COLUMN json TO data;
SELECT id, data.typed, data.arr.size0, data.dynamic.:Int64
FROM json_cached_compact_presence PREWHERE data.typed > 0 ORDER BY id;

DROP TABLE json_cached_compact_presence;

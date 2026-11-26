-- Verify that `system.dictionaries.bytes_allocated` accounts for the lookup index built by `polygon_index_cell` and
-- `polygon_index_each`. Before the accounting fix, those layouts reported the same number of bytes as `polygon_simple`
-- (which holds no index at all), even though the segment trees and grid cells they build can dominate the
-- dictionary's footprint.

DROP DICTIONARY IF EXISTS polygon_dict_simple;
DROP DICTIONARY IF EXISTS polygon_dict_each;
DROP DICTIONARY IF EXISTS polygon_dict_cell;
DROP TABLE IF EXISTS polygon_dict_bytes_src SYNC;

CREATE TABLE polygon_dict_bytes_src
(
    polygon Array(Array(Array(Tuple(Float64, Float64)))),
    city_id UInt32
) ENGINE = Memory;

-- Small set of disjoint axis-aligned parcels. The grid built by `polygon_index_cell` recurses to its default
-- max depth regardless of the polygon count, so even a small set produces enough leaf cells for the index
-- footprint to clearly dominate the simple layout (which holds no index).
INSERT INTO polygon_dict_bytes_src
SELECT
    [[[
        (cx - 0.0004, cy - 0.0004),
        (cx + 0.0004, cy - 0.0004),
        (cx + 0.0004, cy + 0.0004),
        (cx - 0.0004, cy + 0.0004),
        (cx - 0.0004, cy - 0.0004)
    ]]],
    toUInt32(n)
FROM
(
    SELECT
        number AS n,
        (n %  10) * 0.01 AS cx,
        intDiv(n, 10) * 0.01 AS cy
    FROM numbers(100)
);

CREATE DICTIONARY polygon_dict_simple
(polygon Array(Array(Array(Tuple(Float64, Float64)))), city_id UInt32)
PRIMARY KEY polygon
SOURCE(CLICKHOUSE(TABLE 'polygon_dict_bytes_src'))
LIFETIME(0)
LAYOUT(POLYGON_SIMPLE());

CREATE DICTIONARY polygon_dict_each
(polygon Array(Array(Array(Tuple(Float64, Float64)))), city_id UInt32)
PRIMARY KEY polygon
SOURCE(CLICKHOUSE(TABLE 'polygon_dict_bytes_src'))
LIFETIME(0)
LAYOUT(POLYGON_INDEX_EACH());

CREATE DICTIONARY polygon_dict_cell
(polygon Array(Array(Array(Tuple(Float64, Float64)))), city_id UInt32)
PRIMARY KEY polygon
SOURCE(CLICKHOUSE(TABLE 'polygon_dict_bytes_src'))
LIFETIME(0)
LAYOUT(POLYGON_INDEX_CELL());

SYSTEM RELOAD DICTIONARY polygon_dict_simple;
SYSTEM RELOAD DICTIONARY polygon_dict_each;
SYSTEM RELOAD DICTIONARY polygon_dict_cell;

-- Sanity: the simple layout already has a non-zero footprint from polygons + attribute columns alone.
SELECT bytes_allocated > 0
FROM system.dictionaries
WHERE database = currentDatabase() AND name = 'polygon_dict_simple';

-- Each index-bearing layout must report meaningfully more bytes than `polygon_simple`. Without the
-- accounting fix, all three values were equal (the index storage was not counted at all).
SELECT
    (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'polygon_dict_each')
        > 2 * (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'polygon_dict_simple');

SELECT
    (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'polygon_dict_cell')
        > 2 * (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'polygon_dict_simple');

DROP DICTIONARY polygon_dict_simple;
DROP DICTIONARY polygon_dict_each;
DROP DICTIONARY polygon_dict_cell;
DROP TABLE polygon_dict_bytes_src SYNC;

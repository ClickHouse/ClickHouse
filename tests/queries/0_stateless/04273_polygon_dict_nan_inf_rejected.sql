-- Regression test: dictionary loading must reject polygon vertices with NaN or infinity instead of
-- aborting the server inside the slab index builder. See `IPolygonDictionary::addNewPoint`.
-- The server-side AST fuzzer produced inserts like `(cx, cy + nan)` against the polygon dictionary
-- source table from `04204_polygon_dict_bytes_allocated`, which then tripped a `LOGICAL_ERROR` in
-- `SlabsPolygonIndex::indexBuild` (`Error occurred while building polygon index. Edge ... but found ...`).

DROP DICTIONARY IF EXISTS polygon_dict_nan_each;
DROP DICTIONARY IF EXISTS polygon_dict_inf_cell;
DROP TABLE IF EXISTS polygon_src_nan SYNC;

CREATE TABLE polygon_src_nan
(
    polygon Array(Array(Array(Tuple(Float64, Float64)))),
    city_id UInt32
) ENGINE = Memory;

INSERT INTO polygon_src_nan VALUES ([[[(0.0, 0.0), (1.0, 0.0), (nan, 1.0), (0.0, 1.0), (0.0, 0.0)]]], 1);
INSERT INTO polygon_src_nan VALUES ([[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]], 2);

CREATE DICTIONARY polygon_dict_nan_each
(polygon Array(Array(Array(Tuple(Float64, Float64)))), city_id UInt32)
PRIMARY KEY polygon
SOURCE(CLICKHOUSE(TABLE 'polygon_src_nan'))
LIFETIME(0)
LAYOUT(POLYGON_INDEX_EACH());

-- Must fail at load time with a clear user-facing error, not crash with a LOGICAL_ERROR.
SYSTEM RELOAD DICTIONARY polygon_dict_nan_each; -- { serverError BAD_ARGUMENTS }

TRUNCATE TABLE polygon_src_nan;
INSERT INTO polygon_src_nan VALUES ([[[(0.0, 0.0), (1.0, 0.0), (inf, 1.0), (0.0, 1.0), (0.0, 0.0)]]], 1);
INSERT INTO polygon_src_nan VALUES ([[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]], 2);

CREATE DICTIONARY polygon_dict_inf_cell
(polygon Array(Array(Array(Tuple(Float64, Float64)))), city_id UInt32)
PRIMARY KEY polygon
SOURCE(CLICKHOUSE(TABLE 'polygon_src_nan'))
LIFETIME(0)
LAYOUT(POLYGON_INDEX_CELL());

SYSTEM RELOAD DICTIONARY polygon_dict_inf_cell; -- { serverError BAD_ARGUMENTS }

-- Final sanity: a clean source still loads.
TRUNCATE TABLE polygon_src_nan;
INSERT INTO polygon_src_nan VALUES ([[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]], 1);
SYSTEM RELOAD DICTIONARY polygon_dict_nan_each;
SELECT status = 'LOADED' FROM system.dictionaries WHERE database = currentDatabase() AND name = 'polygon_dict_nan_each';

DROP DICTIONARY polygon_dict_nan_each;
DROP DICTIONARY polygon_dict_inf_cell;
DROP TABLE polygon_src_nan SYNC;

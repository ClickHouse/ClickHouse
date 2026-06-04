-- Polygon coordinates with NaN or infinite components must be rejected with a
-- user error instead of producing a "logical error" while building the index.

DROP DICTIONARY IF EXISTS dict_polygons_nan;
DROP TABLE IF EXISTS polygons_nan;

CREATE TABLE polygons_nan
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO polygons_nan VALUES ([[[(0, 0), (1, 0), (nan, 1), (0, 1)]]], 'with_nan');

CREATE DICTIONARY dict_polygons_nan
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_nan' DB currentDatabase()))
LIFETIME(0)
LAYOUT(POLYGON());

-- Loading the dictionary should fail with a user error, not a logical error.
SELECT dictGet('dict_polygons_nan', 'name', tuple(0.5, 0.5)); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY dict_polygons_nan;
DROP TABLE polygons_nan;


DROP DICTIONARY IF EXISTS dict_polygons_inf;
DROP TABLE IF EXISTS polygons_inf;

CREATE TABLE polygons_inf
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO polygons_inf VALUES ([[[(0, 0), (1, 0), (inf, 1), (0, 1)]]], 'with_inf');

CREATE DICTIONARY dict_polygons_inf
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_inf' DB currentDatabase()))
LIFETIME(0)
LAYOUT(POLYGON());

SELECT dictGet('dict_polygons_inf', 'name', tuple(0.5, 0.5)); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY dict_polygons_inf;
DROP TABLE polygons_inf;

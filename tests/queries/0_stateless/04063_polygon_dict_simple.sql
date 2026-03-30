DROP DICTIONARY IF EXISTS dict_polygons;
DROP TABLE IF EXISTS polygons;

CREATE TABLE polygons
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String,
    value1 UInt64,
    value2 UInt64,
    value3 String
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

-- A unit square [0,1]x[0,1]
INSERT INTO polygons VALUES ([[[(0, 0), (1, 0), (1, 1), (0, 1)]]], 'square', 10, 0, '');
-- A triangle with vertices at (2,0), (4,0), (3,2)
INSERT INTO polygons VALUES ([[[(2, 0), (4, 0), (3, 2)]]], 'triangle', 20, 0, '');
-- A larger rectangle [5,8]x[0,3] with a hole [6,7]x[1,2]
INSERT INTO polygons VALUES ([[[(5, 0), (8, 0), (8, 3), (5, 3)], [(6, 1), (7, 1), (7, 2), (6, 2)]]], 'rect_with_hole', 30, 0, '');

CREATE DICTIONARY dict_polygons
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String,
    value1 UInt64,
    value2 UInt64,
    value3 String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons' DB currentDatabase()))
LIFETIME(0)
LAYOUT(POLYGON());

-- Point inside the square
SELECT 'dictGet', dictGet('dict_polygons', 'name', tuple(0.5, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value1', tuple(0.5, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value2', tuple(0.5, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value3', tuple(0.5, 0.5));

-- Point inside the triangle
SELECT 'dictGet', dictGet('dict_polygons', 'name', tuple(3.0, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value1', tuple(3.0, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value2', tuple(3.0, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value3', tuple(3.0, 0.5));

-- Point inside the rectangle (but outside the hole)
SELECT 'dictGet', dictGet('dict_polygons', 'name', tuple(5.5, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value1', tuple(5.5, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value2', tuple(5.5, 0.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value3', tuple(5.5, 0.5));

-- Point inside the hole of the rectangle
SELECT 'dictGet', dictGet('dict_polygons', 'name', tuple(6.5, 1.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value1', tuple(6.5, 1.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value2', tuple(6.5, 1.5));
SELECT 'dictGet', dictGet('dict_polygons', 'value3', tuple(6.5, 1.5));

-- Point outside all polygons
SELECT 'dictGet', dictGet('dict_polygons', 'name', tuple(10.0, 10.0));
SELECT 'dictGet', dictGet('dict_polygons', 'value1', tuple(10.0, 10.0));
SELECT 'dictGet', dictGet('dict_polygons', 'value2', tuple(10.0, 10.0));
SELECT 'dictGet', dictGet('dict_polygons', 'value3', tuple(10.0, 10.0));

DROP DICTIONARY dict_polygons;
DROP TABLE polygons;

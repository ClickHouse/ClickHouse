-- Tags: no-fasttest

-- example from h3 docs
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells([(-122.4089866999972145,37.813318999983238),(-122.3544736999993603,37.7198061999978478),(-122.4798767000009008,37.8151571999998453)], 7)))
  = ['872830820ffffff','872830828ffffff','87283082affffff','87283082bffffff','87283082effffff','872830870ffffff','872830876ffffff'];

-- test both rings, polygons and multipolygons
DROP TABLE IF EXISTS rings;
DROP TABLE IF EXISTS polygons;
DROP TABLE IF EXISTS multipolygons;

CREATE TABLE rings (ring Ring) ENGINE=Memory();
CREATE TABLE polygons (polygon Polygon) ENGINE=Memory();
CREATE TABLE multipolygons (multipolygon MultiPolygon) ENGINE=Memory();

INSERT INTO rings VALUES ([(55.66824,12.595493),(55.667901,12.593991),(55.667474,12.595117),(55.66824,12.595493)]);
-- expected: '8b63a9a9914cfff','8b63a9a99168fff','8b63a9a9916afff'
INSERT INTO polygons VALUES ([[(55.66824,12.595493),(55.667901,12.593991),(55.667474,12.595117),(55.66824,12.595493)],[(55.667695,12.595026),(55.667953,12.595085),(55.66788,12.594683),(55.667695,12.595026)]]);
-- expected: '8b63a9a9914cfff','8b63a9a99168fff'
INSERT INTO multipolygons VALUES ([[[(55.66824,12.595493),(55.667901,12.593991),(55.667474,12.595117),(55.66824,12.595493)],[(55.667695,12.595026),(55.667953,12.595085),(55.66788,12.594683),(55.667695,12.595026)]], [[(55.668461,12.597461),(55.668446,12.596694),(55.668797,12.596962),(55.668461,12.597461)]] ]);
-- expected: '8b63a9a9914cfff','8b63a9a99168fff', '8b63a9a99bb3fff'

SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(ring, 11)))
  = ['8b63a9a9914cfff','8b63a9a99168fff','8b63a9a9916afff'] FROM rings;
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(polygon, 11)))
  = ['8b63a9a9914cfff','8b63a9a99168fff'] FROM polygons;
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCells(multipolygon, 11)))
  = ['8b63a9a9914cfff','8b63a9a99168fff','8b63a9a99bb3fff'] FROM multipolygons;

DROP TABLE rings;
DROP TABLE polygons;
DROP TABLE multipolygons;

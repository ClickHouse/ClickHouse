SELECT 'boundingBoxCartesian Ring';
SELECT boundingBoxCartesian([(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]);
SELECT 'boundingBoxSpherical Ring';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical([(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]));

SELECT 'boundingBoxCartesian Polygon';
SELECT boundingBoxCartesian([[(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]]);
SELECT 'boundingBoxSpherical Polygon';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical([[(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]]));

SELECT 'boundingBoxCartesian Point';
SELECT boundingBoxCartesian((3., 4.));

SELECT 'boundingBoxCartesian LineString';
SELECT boundingBoxCartesian(CAST([(1., 2.), (5., 8.)], 'LineString'));

SELECT 'boundingBoxCartesian MultiPolygon';
SELECT boundingBoxCartesian([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(5., 5.), (5., 7.), (8., 7.), (8., 5.), (5., 5.)]]]);

SELECT 'boundingBoxCartesian WKT';
SELECT boundingBoxCartesian(readWKTPolygon('POLYGON((1 1, 1 3, 4 3, 4 1, 1 1))'));

SELECT 'boundingBoxSpherical Ring';
SELECT boundingBoxSpherical([(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]);

SELECT 'boundingBoxSpherical Polygon';
SELECT boundingBoxSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]);

SELECT 'boundingBoxSpherical Point';
SELECT boundingBoxSpherical((4.35, 50.85));

SELECT 'boundingBoxSpherical LineString';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical(CAST([(0., 50.), (10., 50.)], 'LineString')));

SELECT 'Point near date line (179°)';
SELECT boundingBoxSpherical((179., 50.));

SELECT 'Point near date line (-179°)';
SELECT boundingBoxSpherical((-179., 50.));

SELECT 'LineString crossing date line (179° to -179°)';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical(CAST([(179., 50.), (-179., 50.)], 'LineString')));

SELECT 'Polygon crossing date line';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical([[(178., 45.), (180., 45.), (-178., 45.), (-180., 45.), (-180., 55.), (180., 55.), (178., 55.), (-178., 55.), (178., 45.)]]));

SELECT 'Narrow polygon crossing date line (175° to -175°)';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical([[(175., 50.), (180., 50.), (-175., 50.), (-180., 50.), (-180., 55.), (180., 55.), (175., 55.), (-175., 55.), (175., 50.)]]));

SELECT 'Same geometry in Cartesian (no date line issue)';
SELECT boundingBoxCartesian([[(178., 45.), (180., 45.), (-178., 45.), (-180., 45.), (-180., 55.), (180., 55.), (178., 55.), (-178., 55.), (178., 45.)]]);

SELECT 'MultiPolygon across date line';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical([[[(179., 50.), (180., 50.), (180., 51.), (179., 51.), (179., 50.)]], [[(-180., 50.), (-179., 50.), (-179., 51.), (-180., 51.), (-180., 50.)]]]));

SELECT 'boundingBoxCartesian Polygon with hole';
SELECT boundingBoxCartesian([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]);

SELECT 'boundingBoxSpherical Polygon with hole';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]));

SELECT 'boundingBoxCartesian negative coordinates';
SELECT boundingBoxCartesian([(-5., -5.), (-5., 0.), (0., 0.), (0., -5.), (-5., -5.)]);

SELECT 'boundingBoxSpherical negative coordinates';
SELECT arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical([(-5., -5.), (-5., 0.), (0., 0.), (0., -5.), (-5., -5.)]));

SELECT 'boundingBoxCartesian large coordinates';
SELECT boundingBoxCartesian([(1e10, 1e10), (1e10, 2e10), (2e10, 2e10), (2e10, 1e10), (1e10, 1e10)]);

SELECT 'boundingBoxCartesian degenerate ring (all same points)';
SELECT boundingBoxCartesian([(5., 5.), (5., 5.), (5., 5.)]);

SELECT 'boundingBoxCartesian collinear points';
SELECT boundingBoxCartesian([(0., 0.), (5., 5.), (10., 10.), (0., 0.)]);

SELECT 'boundingBoxCartesian WKT MultiPolygon';
SELECT boundingBoxCartesian(readWKTMultiPolygon('MULTIPOLYGON(((0 0, 0 2, 2 2, 2 0, 0 0)), ((5 5, 5 7, 7 7, 7 5, 5 5)))'));

SELECT 'boundingBoxCartesian Geometry Point';
SELECT boundingBoxCartesian(readWKT('POINT(3 4)'));
SELECT 'boundingBoxCartesian Geometry LineString';
SELECT boundingBoxCartesian(readWKT('LINESTRING(1 2, 5 8)'));
SELECT 'boundingBoxCartesian Geometry Polygon';
SELECT boundingBoxCartesian(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))'));
SELECT 'boundingBoxCartesian Geometry MultiPolygon';
SELECT boundingBoxCartesian(readWKT('MULTIPOLYGON(((0 0, 0 2, 2 2, 2 0, 0 0)), ((5 5, 5 7, 7 7, 7 5, 5 5)))'));

SELECT boundingBoxCartesian(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'centroidCartesian Point';
SELECT centroidCartesian((2., 3.));

SELECT 'centroidCartesian Ring';
SELECT centroidCartesian([(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]);

SELECT 'centroidCartesian Polygon';
SELECT centroidCartesian([[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]]);

SELECT 'centroidCartesian LineString';
SELECT centroidCartesian(CAST([(0., 0.), (4., 0.)], 'LineString'));

SELECT 'centroidCartesian MultiLineString';
SELECT centroidCartesian(CAST([[(0., 0.), (2., 0.)], [(2., 0.), (4., 0.)]], 'MultiLineString'));

SELECT 'centroidCartesian MultiPolygon';
SELECT centroidCartesian([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (4., 6.), (6., 6.), (6., 4.), (4., 4.)]]]);

SELECT 'centroidCartesian WKT';
SELECT centroidCartesian(readWKTPolygon('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))'));

SELECT 'centroidCartesian triangle';
SELECT centroidCartesian([(0., 0.), (6., 0.), (3., 6.), (0., 0.)]);

SELECT 'centroidCartesian Polygon with hole';
SELECT centroidCartesian([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]);

SELECT 'centroidCartesian negative coordinates';
SELECT centroidCartesian([(-5., -5.), (-5., 0.), (0., 0.), (0., -5.), (-5., -5.)]);

SELECT 'centroidCartesian large coordinates';
SELECT centroidCartesian([(1e10, 1e10), (1e10, 2e10), (2e10, 2e10), (2e10, 1e10), (1e10, 1e10)]);

SELECT 'centroidCartesian degenerate ring (all same points)';
SELECT centroidCartesian([(5., 5.), (5., 5.), (5., 5.)]);

SELECT 'centroidCartesian collinear points';
SELECT centroidCartesian([(0., 0.), (5., 5.), (10., 10.), (0., 0.)]);

SELECT 'centroidCartesian WKT MultiPolygon';
SELECT centroidCartesian(readWKTMultiPolygon('MULTIPOLYGON(((0 0, 0 2, 2 2, 2 0, 0 0)), ((5 5, 5 7, 7 7, 7 5, 5 5)))'));

SELECT 'centroidCartesian Geometry Point';
SELECT centroidCartesian(readWKT('POINT(2 3)'));
SELECT 'centroidCartesian Geometry LineString';
SELECT centroidCartesian(readWKT('LINESTRING(0 0, 4 0)'));
SELECT 'centroidCartesian Geometry Polygon';
SELECT centroidCartesian(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))'));
SELECT 'centroidCartesian Geometry MultiPolygon';
SELECT centroidCartesian(readWKT('MULTIPOLYGON(((0 0, 0 2, 2 2, 2 0, 0 0)), ((4 4, 4 6, 6 6, 6 4, 4 4)))'));

SELECT centroidCartesian(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'centroidCartesian empty polygon (degenerate)';
SELECT centroidCartesian([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]);

SELECT 'centroidCartesian single-point linestring (degenerate)';
SELECT centroidCartesian(CAST([(3., 7.)], 'LineString'));

SELECT 'boundingBoxCartesian MultiPoint';
SELECT boundingBoxCartesian(CAST([(0., 0.), (3., 5.), (6., 0.)], 'MultiPoint'));

SELECT 'boundingBoxSpherical MultiPoint';
SELECT boundingBoxSpherical(CAST([(0., 0.), (3., 5.), (6., 0.)], 'MultiPoint'));

SELECT 'boundingBoxSpherical MultiPoint crossing date line';
SELECT boundingBoxSpherical(CAST([(179., 50.), (-179., 50.)], 'MultiPoint'));

SELECT 'boundingBoxCartesian Geometry MultiPoint';
SELECT boundingBoxCartesian(readWKT('MULTIPOINT (0 0, 3 5, 6 0)'));

SELECT 'centroidCartesian MultiPoint';
SELECT centroidCartesian(CAST([(0., 0.), (3., 5.), (6., 0.)], 'MultiPoint'));

SELECT 'centroidCartesian Geometry MultiPoint';
SELECT centroidCartesian(readWKT('MULTIPOINT (0 0, 3 5, 6 0)'));

SELECT 'centroidCartesian single-point MultiPoint (degenerate)';
SELECT centroidCartesian(CAST([(3., 7.)], 'MultiPoint'));

SELECT 'centroidCartesian empty MultiPoint (degenerate)';
SELECT centroidCartesian(CAST([], 'MultiPoint'));

SELECT 'centroidCartesian Polygon with off-center hole of the same winding as the outer ring';
SELECT centroidCartesian([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)]]);
SELECT centroidCartesian(readWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))'));

SELECT 'centroidCartesian Polygon with unclosed hole';
SELECT centroidCartesian([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)]]);

SELECT 'centroidCartesian MultiPolygon with mixed winding';
SELECT centroidCartesian([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (6., 4.), (6., 6.), (4., 6.), (4., 4.)]]]);
SELECT centroidCartesian(readWKT('MULTIPOLYGON(((0 0, 0 2, 2 2, 2 0, 0 0)), ((4 4, 6 4, 6 6, 4 6, 4 4)))'));

-- A block with more rows than member geometries: an empty row must not shift or drop the following rows.
SELECT 'MultiLineString empty row followed by nonempty rows';
SELECT g, centroidCartesian(g), boundingBoxCartesian(g), boundingBoxSpherical(g)
FROM (SELECT CAST(arrayJoin([[], [[(10., 10.), (12., 12.)]]]), 'MultiLineString') AS g);

SELECT 'MultiPolygon empty row followed by nonempty rows';
SELECT g, centroidCartesian(g), boundingBoxCartesian(g), arrayMap(p -> (p.1, round(p.2, 10)), boundingBoxSpherical(g)), polygonAreaCartesian(g)
FROM (SELECT CAST(arrayJoin([[], [[[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]]]]), 'MultiPolygon') AS g);

-- A ring of zero area has the centroid of its boundary, which does not depend on the starting vertex.
SELECT 'centroidCartesian collinear points, other starting vertices';
SELECT centroidCartesian([(5., 5.), (10., 10.), (0., 0.), (5., 5.)]), centroidCartesian([(10., 10.), (0., 0.), (5., 5.), (10., 10.)]);

SELECT 'centroidCartesian polygon with a hole equal to the outer ring (zero area)';
SELECT centroidCartesian([[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)], [(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]]);

SELECT 'centroidCartesian tiny square';
SELECT round(c.1, 15), round(c.2, 15) FROM (SELECT centroidCartesian([(0., 0.), (0., 1e-9), (1e-9, 1e-9), (1e-9, 0.), (0., 0.)]) AS c);

SELECT 'centroidCartesian of empty geometries';
SELECT centroidCartesian(CAST([], 'Ring')), centroidCartesian(CAST([], 'Polygon')), centroidCartesian(CAST([], 'LineString')), centroidCartesian(CAST([], 'MultiPolygon'));

SELECT 'boundingBoxCartesian of empty geometries';
SELECT boundingBoxCartesian(CAST([], 'Ring')), boundingBoxCartesian(CAST([], 'Polygon')), boundingBoxCartesian(CAST([], 'MultiPoint')), boundingBoxCartesian(CAST([], 'LineString'));

SELECT 'boundingBoxSpherical of empty geometries';
SELECT boundingBoxSpherical(CAST([], 'Ring')), boundingBoxSpherical(CAST([], 'Polygon')), boundingBoxSpherical(CAST([], 'MultiPoint')), boundingBoxSpherical(CAST([], 'LineString'));

SELECT 'NULL Geometry';
SELECT centroidCartesian(CAST(NULL, 'Geometry')), boundingBoxCartesian(CAST(NULL, 'Geometry')), boundingBoxSpherical(CAST(NULL, 'Geometry'));

-- Like typed columns, `Geometry` rejects NaN and infinite coordinates.
SELECT centroidCartesian(CAST(CAST((nan, 1.), 'Point'), 'Geometry')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT centroidCartesian(CAST(CAST([[(0., 0.), (nan, 1.), (1., 1.), (0., 0.)]], 'Polygon'), 'Geometry')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT boundingBoxCartesian(CAST(CAST([(0., 0.), (inf, 1.)], 'LineString'), 'Geometry')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT boundingBoxSpherical(CAST(CAST([[(0., 0.), (1., nan), (1., 1.), (0., 0.)]], 'Polygon'), 'Geometry')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'boundingBoxSpherical ring around a pole, in both orientations';
SELECT boundingBoxSpherical([(0., 89.), (120., 89.), (240., 89.), (0., 89.)]);
SELECT boundingBoxSpherical([(0., 89.), (240., 89.), (120., 89.), (0., 89.)]);
SELECT boundingBoxSpherical([[(0., -80.), (-120., -80.), (-240., -80.), (0., -80.)]]);
SELECT boundingBoxSpherical([[(0., -80.), (-240., -80.), (-120., -80.), (0., -80.)]]);

SELECT 'boundingBoxSpherical latitude at the limits';
SELECT boundingBoxSpherical(CAST([(0., -90.), (0., 90.)], 'MultiPoint'));
SELECT boundingBoxSpherical([(0., 0.), (0., 100.), (10., 0.), (0., 0.)]); -- { serverError BAD_ARGUMENTS }
SELECT boundingBoxSpherical((0., -90.5)); -- { serverError BAD_ARGUMENTS }
SELECT boundingBoxSpherical(readWKT('LINESTRING(0 0, 10 91)')); -- { serverError BAD_ARGUMENTS }

SELECT 'boundingBoxSpherical of an edge between antipodal points';
SELECT boundingBoxSpherical(CAST([(0., 0.), (180., 0.)], 'LineString')); -- { serverError BAD_ARGUMENTS }
SELECT boundingBoxSpherical([[(0., -90.), (10., 0.), (20., 90.)]]); -- { serverError BAD_ARGUMENTS }
SELECT boundingBoxSpherical(CAST([(0., 0.), (179., 0.)], 'LineString'));

-- The ring splits the sphere into two equal halves, so its interior is the half on its right whatever the winding.
SELECT 'boundingBoxSpherical of a centrally symmetric ring, in both orientations';
SELECT arrayMap(p -> (round(p.1, 6), round(p.2, 6)), boundingBoxSpherical([(0., 0.), (45., 30.), (90., 45.), (135., 30.), (180., 0.), (-135., -30.), (-90., -45.), (-45., -30.)]));
SELECT arrayMap(p -> (round(p.1, 6), round(p.2, 6)), boundingBoxSpherical([(0., 0.), (-45., -30.), (-90., -45.), (-135., -30.), (180., 0.), (135., 30.), (90., 45.), (45., 30.)]));

-- Whether a pole is inside a ring does not depend on the starting vertex or the winding, also for an irregular ring.
SELECT 'boundingBoxSpherical irregular ring around a pole, all starting vertices and both windings';
WITH [(133.55150700883996, -5.594781178221163), (-16.874320314874183, -4.136082718699165), (-177.39630222353685, -2.2380850832444468)] AS ring
SELECT number, boundingBoxSpherical(arrayRotateLeft(ring, number)), boundingBoxSpherical(arrayReverse(arrayRotateLeft(ring, number)))
FROM numbers(3)
ORDER BY number;

SELECT 'boundingBoxSpherical rings with a vertex at a pole';
SELECT boundingBoxSpherical([(0., 90.), (0., 0.), (90., 0.)]);
SELECT boundingBoxSpherical([(90., 0.), (0., 0.), (0., 90.)]);
SELECT boundingBoxSpherical([(0., 90.), (0., 0.), (0., -90.), (90., 0.)]);
SELECT boundingBoxSpherical([(0., 90.), (90., 0.), (0., -90.), (0., 0.)]);
-- Two equal halves: the interior is the half on the right.
SELECT boundingBoxSpherical([(0., 90.), (0., 0.), (0., -90.), (180., 0.)]);
SELECT boundingBoxSpherical([(0., 90.), (180., 0.), (0., -90.), (0., 0.)]);

SELECT 'boundingBoxSpherical ring that goes to a pole and back along the date line';
SELECT boundingBoxSpherical([(-180., -60.), (-90., -60.), (0., -60.), (90., -60.), (180., -60.), (180., -90.), (-180., -90.)]);
SELECT boundingBoxSpherical([(-180., -90.), (180., -90.), (180., -60.), (90., -60.), (0., -60.), (-90., -60.), (-180., -60.)]);

-- The edge passes through the north pole along the meridians 0 and 180, but not across the longitudes between them.
SELECT 'boundingBoxSpherical edge through a pole';
SELECT boundingBoxSpherical(CAST([[(0., 80.), (180., 80.)], [(190., 0.), (200., 0.)]], 'MultiLineString'));
SELECT boundingBoxSpherical(CAST([[(190., 0.), (200., 0.)], [(180., 80.), (0., 80.)]], 'MultiLineString'));

SELECT 'boundingBoxSpherical polygon with a hole around a pole';
SELECT arrayMap(p -> (round(p.1, 6), round(p.2, 6)), boundingBoxSpherical([[(0., 60.), (90., 60.), (180., 60.), (-90., 60.)], [(0., 80.), (90., 80.), (180., 80.), (-90., 80.)]]));
SELECT arrayMap(p -> (round(p.1, 6), round(p.2, 6)), boundingBoxSpherical([[(-90., 60.), (180., 60.), (90., 60.), (0., 60.)], [(-90., 80.), (180., 80.), (90., 80.), (0., 80.)]]));

SELECT 'boundingBoxSpherical MultiPolygon with a pole, in both orders of the parts';
SELECT boundingBoxSpherical([[[(0., 80.), (120., 80.), (240., 80.)]], [[(10., 10.), (20., 10.), (20., 20.)]]]);
SELECT boundingBoxSpherical([[[(10., 10.), (20., 10.), (20., 20.)]], [[(0., 80.), (240., 80.), (120., 80.)]]]);

-- The edge passes a few ulp away from the south pole. Its box does not change when the longitudes are shifted by 360.
SELECT 'boundingBoxSpherical edge near a pole';
SELECT arrayMap(p -> (round(p.1, 6), round(p.2, 6)), boundingBoxSpherical(CAST([(146., -45.), (-34.00000000000003, -31.)], 'LineString')));
SELECT arrayMap(p -> (round(p.1, 6), round(p.2, 6)), boundingBoxSpherical(CAST([(120., 14.), (-60.00000000000003, 71.)], 'LineString')));
SELECT boundingBoxSpherical(CAST([(-450.0073801500112, -42.851340024819784), (-630.0073801500112, -31.425670012409892)], 'LineString'))
    = boundingBoxSpherical(CAST([(-90.00738015001122, -42.851340024819784), (89.99261984998884, -31.425670012409892)], 'LineString'));

-- The lowest point of the long edge is between its ends, 5.67e-7 degrees south of the equator.
SELECT 'boundingBoxSpherical long edge near the equator';
SELECT round(boundingBoxSpherical(CAST([(-130.91979848301776, -5.8858487778102766e-09), (48.556966476834276, 7.054831480445877e-10)], 'LineString'))[1].2 * 1e9, 3);

-- The boxes of the edges meet at exact longitudes, so the box covers all longitudes.
SELECT 'boundingBoxSpherical all longitudes';
SELECT arrayMap(p -> (p.1, round(p.2, 6)), boundingBoxSpherical(CAST([(-175.794589973597, 31.049297105635784), (154.71421351483707, -5.355131697376247), (-6.150296442507331, 50.22436697904284), (177.84811019335774, -7.5178911688345575), (357.84811019335774, -13.75894558441728)], 'LineString')));

-- As in `boundingBoxCartesian`, the box of a polygon without an outer ring is the box of its holes.
SELECT 'boundingBoxSpherical polygon without an outer ring';
SELECT arrayMap(p -> (round(p.1, 6), round(p.2, 6)), boundingBoxSpherical(CAST([[], [(1., 1.), (2., 3.), (3., 1.)]], 'Polygon')));
SELECT boundingBoxCartesian(CAST([[], [(1., 1.), (2., 3.), (3., 1.)]], 'Polygon'));

-- Each ring has its own origin, so far apart parts keep their precision and the area is not taken as zero.
SELECT 'centroidCartesian small far apart squares, in both orders';
WITH [[(0., 0.), (1e-6, 0.), (1e-6, 1e-6), (0., 1e-6)]] AS a, [[(100., 50.), (100.000003, 50.), (100.000003, 50.000003), (100., 50.000003)]] AS b
SELECT round(centroidCartesian([a, b]).1, 6), round(centroidCartesian([a, b]).2, 6), centroidCartesian([a, b]) = centroidCartesian([b, a]);

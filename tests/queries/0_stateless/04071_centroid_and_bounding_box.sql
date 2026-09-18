SELECT 'boundingBoxCartesian Ring';
SELECT boundingBoxCartesian([(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]);
SELECT 'boundingBoxSpherical Ring';
SELECT boundingBoxSpherical([(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]);

SELECT 'boundingBoxCartesian Polygon';
SELECT boundingBoxCartesian([[(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]]);
SELECT 'boundingBoxSpherical Polygon';
SELECT boundingBoxSpherical([[(-40., 60.), (40., 60.), (0., 50.), (-40., 60.)]]);

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
SELECT boundingBoxSpherical(CAST([(0., 50.), (10., 50.)], 'LineString'));

SELECT 'Point near date line (179°)';
SELECT boundingBoxSpherical((179., 50.));

SELECT 'Point near date line (-179°)';
SELECT boundingBoxSpherical((-179., 50.));

SELECT 'LineString crossing date line (179° to -179°)';
SELECT boundingBoxSpherical(CAST([(179., 50.), (-179., 50.)], 'LineString'));

SELECT 'Polygon crossing date line';
SELECT boundingBoxSpherical([[(178., 45.), (180., 45.), (-178., 45.), (-180., 45.), (-180., 55.), (180., 55.), (178., 55.), (-178., 55.), (178., 45.)]]);

SELECT 'Narrow polygon crossing date line (175° to -175°)';
SELECT boundingBoxSpherical([[(175., 50.), (180., 50.), (-175., 50.), (-180., 50.), (-180., 55.), (180., 55.), (175., 55.), (-175., 55.), (175., 50.)]]);

SELECT 'Same geometry in Cartesian (no date line issue)';
SELECT boundingBoxCartesian([[(178., 45.), (180., 45.), (-178., 45.), (-180., 45.), (-180., 55.), (180., 55.), (178., 55.), (-178., 55.), (178., 45.)]]);

SELECT 'MultiPolygon across date line';
SELECT boundingBoxSpherical([[[(179., 50.), (180., 50.), (180., 51.), (179., 51.), (179., 50.)]], [[(-180., 50.), (-179., 50.), (-179., 51.), (-180., 51.), (-180., 50.)]]]);

SELECT 'boundingBoxCartesian Polygon with hole';
SELECT boundingBoxCartesian([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]);

SELECT 'boundingBoxSpherical Polygon with hole';
SELECT boundingBoxSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]);

SELECT 'boundingBoxCartesian negative coordinates';
SELECT boundingBoxCartesian([(-5., -5.), (-5., 0.), (0., 0.), (0., -5.), (-5., -5.)]);

SELECT 'boundingBoxSpherical negative coordinates';
SELECT boundingBoxSpherical([(-5., -5.), (-5., 0.), (0., 0.), (0., -5.), (-5., -5.)]);

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

SELECT 'centroidSpherical Point';
SELECT centroidSpherical((2., 3.));

SELECT 'centroidSpherical Ring';
SELECT round(centroidSpherical([(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]).1, 6), round(centroidSpherical([(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]).2, 6);

SELECT 'centroidSpherical Polygon';
SELECT round(centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]).1, 6), round(centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]).2, 6);

SELECT 'centroidSpherical LineString';
SELECT round(centroidSpherical(CAST([(0., 0.), (10., 0.)], 'LineString')).1, 6), round(centroidSpherical(CAST([(0., 0.), (10., 0.)], 'LineString')).2, 6);

SELECT 'centroidSpherical MultiLineString';
SELECT round(centroidSpherical(CAST([[(0., 0.), (5., 0.)], [(5., 0.), (10., 0.)]], 'MultiLineString')).1, 6), round(centroidSpherical(CAST([[(0., 0.), (5., 0.)], [(5., 0.), (10., 0.)]], 'MultiLineString')).2, 6);

SELECT 'centroidSpherical MultiPolygon';
SELECT round(centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (4., 6.), (6., 6.), (6., 4.), (4., 4.)]]]).1, 6), round(centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (4., 6.), (6., 6.), (6., 4.), (4., 4.)]]]).2, 6);

SELECT 'centroidSpherical date line crossing LineString';
SELECT round(centroidSpherical(CAST([(179., 0.), (-179., 0.)], 'LineString')).1, 6), round(centroidSpherical(CAST([(179., 0.), (-179., 0.)], 'LineString')).2, 6);

SELECT 'centroidSpherical near North Pole';
SELECT round(centroidSpherical([(0., 89.), (120., 89.), (240., 89.), (0., 89.)]).1, 6), round(centroidSpherical([(0., 89.), (120., 89.), (240., 89.), (0., 89.)]).2, 6);

SELECT 'centroidSpherical Polygon with hole';
SELECT round(centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]).1, 6), round(centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]).2, 6);

SELECT 'centroidSpherical square';
SELECT round(centroidSpherical([(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]).1, 6), round(centroidSpherical([(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]).2, 6);

SELECT 'centroidSpherical Geometry Point';
SELECT centroidSpherical(readWKT('POINT(2 3)'));
SELECT 'centroidSpherical Geometry Polygon';
SELECT round(centroidSpherical(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))')).1, 6), round(centroidSpherical(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))')).2, 6);

SELECT 'centroidSpherical globe-spanning polygon (triggers adaptive origin)';
SELECT round(centroidSpherical([(0., 0.), (90., 0.), (180., 0.), (-90., 0.), (0., 0.)]).1, 6), round(centroidSpherical([(0., 0.), (90., 0.), (180., 0.), (-90., 0.), (0., 0.)]).2, 6);

SELECT centroidSpherical(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'centroidCartesian empty polygon (degenerate)';
SELECT centroidCartesian([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]);

SELECT 'centroidCartesian single-point linestring (degenerate)';
SELECT centroidCartesian(CAST([(3., 7.)], 'LineString'));

SELECT 'centroidSpherical empty polygon (degenerate)';
SELECT round(centroidSpherical([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]).1, 6), round(centroidSpherical([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]).2, 6);


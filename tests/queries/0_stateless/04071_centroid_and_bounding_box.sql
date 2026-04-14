SELECT 'boundingBoxCartesian Ring';
SELECT boundingBoxCartesian([(0., 0.), (3., 5.), (6., 0.), (0., 0.)]);

SELECT 'boundingBoxCartesian Polygon';
SELECT boundingBoxCartesian([[(0., 0.), (3., 5.), (6., 0.), (0., 0.)]]);

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

SELECT boundingBoxCartesian(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

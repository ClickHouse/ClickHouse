SELECT pointInPolygonFranklin(tuple(2.0,1.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygonFranklin(tuple(1.0,2.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygonFranklin(tuple(4.0,1.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygon(tuple(2.0,1.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygon(tuple(1.0,2.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygon(tuple(4.0,1.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygonWinding(tuple(2.0,1.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygonWinding(tuple(1.0,2.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));
SELECT pointInPolygonWinding(tuple(4.0,1.0), tuple(0.0,0.0), tuple(3.0,3.0), tuple(3.0,0.0), tuple(0.0,0.0));

SELECT 'inner';
SELECT pointInPolygonWithGrid(3., 3., [(6., 0.), (8., 4.), (5., 8.), (0., 2.), (6., 0.)]);
SELECT 'outer';
SELECT pointInPolygonWithGrid(0.1, 0.1, [(6., 0.), (8., 4.), (5., 8.), (0., 2.), (6., 0.)]);
SELECT 'single line';
SELECT pointInPolygonWithGrid(4.1, 0.1, [(6., 0.), (8., 4.), (5., 8.), (0., 2.), (6., 0.)]);
SELECT pointInPolygonWithGrid(4.9, 0.9, [(6., 0.), (8., 4.), (5., 8.), (0., 2.), (6., 0.)]);

SELECT 'pair of lines, single polygon';
SELECT pointInPolygonWithGrid(0.1, 0.1, [(0., 0.), (8., 7.), (7., 8.), (0., 0.)]);
SELECT pointInPolygonWithGrid(0.9, 0.1, [(0., 0.), (8., 7.), (7., 8.), (0., 0.)]);
SELECT pointInPolygonWithGrid(0.1, 0.9, [(0., 0.), (8., 7.), (7., 8.), (0., 0.)]);
SELECT pointInPolygonWithGrid(2.2, 2.2, [(0., 0.), (8., 7.), (7., 8.), (0., 0.)]);
SELECT pointInPolygonWithGrid(2.1, 2.9, [(0., 0.), (8., 7.), (7., 8.), (0., 0.)]);
SELECT pointInPolygonWithGrid(2.9, 2.1, [(0., 0.), (8., 7.), (7., 8.), (0., 0.)]);

SELECT 'pair of lines, different polygons';
SELECT pointInPolygonWithGrid(0.1, 0.1, [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);
SELECT pointInPolygonWithGrid(1., 1., [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);
SELECT pointInPolygonWithGrid(0.7, 0.1, [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);
SELECT pointInPolygonWithGrid(0.1, 0.7, [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);
SELECT pointInPolygonWithGrid(1.1, 0.1, [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);
SELECT pointInPolygonWithGrid(0.1, 1.1, [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);
SELECT pointInPolygonWithGrid(5.0, 5.0, [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);
SELECT pointInPolygonWithGrid(7.9, 7.9, [(0.5, 0.), (1.0, 0.), (8.0, 7.5), (7.5, 8.0), (0., 1.), (0., 0.5), (4.5, 5.5), (5.5, 4.5), (0.5, 0.0)]);

SELECT 'complex polygon';
SELECT pointInPolygonWithGrid(0.05, 0.05, [(0., 1.), (0.2, 0.5), (0.6, 0.5), (0.8, 0.8), (0.8, 0.3), (0.1, 0.3), (0.1, 0.1), (0.8, 0.1), (1.0, 0.0), (8.0, 7.0), (7.0, 8.0), (0., 1.)]);
SELECT pointInPolygonWithGrid(0.15, 0.15, [(0., 1.), (0.2, 0.5), (0.6, 0.5), (0.8, 0.8), (0.8, 0.3), (0.1, 0.3), (0.1, 0.1), (0.8, 0.1), (1.0, 0.0), (8.0, 7.0), (7.0, 8.0), (0., 1.)]);
SELECT pointInPolygonWithGrid(0.3, 0.4, [(0., 1.), (0.2, 0.5), (0.6, 0.5), (0.8, 0.8), (0.8, 0.3), (0.1, 0.3), (0.1, 0.1), (0.8, 0.1), (1.0, 0.0), (8.0, 7.0), (7.0, 8.0), (0., 1.)]);
SELECT pointInPolygonWithGrid(0.4, 0.7, [(0., 1.), (0.2, 0.5), (0.6, 0.5), (0.8, 0.8), (0.8, 0.3), (0.1, 0.3), (0.1, 0.1), (0.8, 0.1), (1.0, 0.0), (8.0, 7.0), (7.0, 8.0), (0., 1.)]);
SELECT pointInPolygonWithGrid(0.7, 0.6, [(0., 1.), (0.2, 0.5), (0.6, 0.5), (0.8, 0.8), (0.8, 0.3), (0.1, 0.3), (0.1, 0.1), (0.8, 0.1), (1.0, 0.0), (8.0, 7.0), (7.0, 8.0), (0., 1.)]);
SELECT pointInPolygonWithGrid(0.9, 0.1, [(0., 1.), (0.2, 0.5), (0.6, 0.5), (0.8, 0.8), (0.8, 0.3), (0.1, 0.3), (0.1, 0.1), (0.8, 0.1), (1.0, 0.0), (8.0, 7.0), (7.0, 8.0), (0., 1.)]);

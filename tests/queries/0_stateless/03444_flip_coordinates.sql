-- {echo}
SELECT flipCoordinates(CAST((10.0, 20.0) AS Point));

SELECT flipCoordinates(CAST([(10, 20), (30, 40), (50, 60)] AS LineString));

WITH CAST([[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]] AS Polygon) AS poly
SELECT flipCoordinates(poly);

WITH CAST([[[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]]] AS MultiPolygon) AS mpoly
SELECT flipCoordinates(mpoly);

WITH CAST([
    [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)],
    [(25, 25), (75, 25), (75, 75), (25, 75), (25, 25)]
] AS Polygon) AS poly_with_hole
SELECT flipCoordinates(poly_with_hole);

WITH CAST([[(10, 20), (30, 40)], [(50, 60), (70, 80)]] AS MultiLineString) AS multiline
SELECT flipCoordinates(multiline);

SELECT flipCoordinates(([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]::MultiPolygon));

SELECT flipCoordinates((1.23, 4.56)::Point), (([(1.23, 4.56)::Point, (2.34, 5.67)::Point])::Ring);

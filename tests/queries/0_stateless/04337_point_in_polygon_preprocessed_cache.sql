-- Constant polygons are preprocessed and shared between queries and threads
-- through a bounded cache. These queries exercise the cached path with
-- multiple threads and several distinct polygons: a polygon, the same polygon
-- again (served from the cache), a polygon with a hole, and a multipolygon.
-- Points are placed strictly inside or outside, away from the edges.

-- Square (0.5, 0.5) - (8.5, 8.5): integer points with x, y in [1, 8] are inside.
SELECT sum(pointInPolygon((number % 10, intDiv(number, 10) % 10), [(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)]))
FROM numbers_mt(1000000)
SETTINGS max_threads = 8, max_block_size = 4096;

-- The same polygon once more: served from the cache, same result.
SELECT sum(pointInPolygon((number % 10, intDiv(number, 10) % 10), [(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)]))
FROM numbers_mt(1000000)
SETTINGS max_threads = 8, max_block_size = 4096;

-- The same square with a hole (3.5, 3.5) - (5.5, 5.5): excludes x, y in [4, 5].
SELECT sum(pointInPolygon((number % 10, intDiv(number, 10) % 10), [[(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)], [(3.5, 3.5), (5.5, 3.5), (5.5, 5.5), (3.5, 5.5)]]))
FROM numbers_mt(1000000)
SETTINGS max_threads = 8, max_block_size = 4096;

-- Multipolygon of two squares (0.5, 0.5) - (3.5, 3.5) and (4.5, 4.5) - (8.5, 8.5):
-- inside for x, y in [1, 3] and for x, y in [5, 8].
SELECT sum(pointInPolygon((number % 10, intDiv(number, 10) % 10), [[[(0.5, 0.5), (3.5, 0.5), (3.5, 3.5), (0.5, 3.5)]], [[(4.5, 4.5), (8.5, 4.5), (8.5, 8.5), (4.5, 8.5)]]]))
FROM numbers_mt(1000000)
SETTINGS max_threads = 8, max_block_size = 4096;

-- Constant point and constant polygon.
SELECT pointInPolygon((2., 2.), [(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)]);
SELECT pointInPolygon((2., 2.), [[[(0.5, 0.5), (3.5, 0.5), (3.5, 3.5), (0.5, 3.5)]], [[(4.5, 4.5), (8.5, 4.5), (8.5, 8.5), (4.5, 8.5)]]]);

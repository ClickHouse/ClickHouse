-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

DROP TABLE IF EXISTS points;
CREATE TABLE points(`x` Float64, `y` Float64) ENGINE = MergeTree ORDER BY (x, y) SETTINGS index_granularity = 1000;   

INSERT INTO points SELECT number, number FROM numbers(100000);

SELECT count()
FROM points
WHERE pointInPolygon((x, y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);

EXPLAIN indexes = 1
SELECT count()
FROM points
WHERE pointInPolygon((x, y), [(0, 0), (0, 25000), (25000, 25000), (25000, 0)]);

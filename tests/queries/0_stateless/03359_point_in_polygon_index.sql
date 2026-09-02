DROP TABLE IF EXISTS t_point_in_polygon;

CREATE TABLE t_point_in_polygon (a UInt64, p Point) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_point_in_polygon (a) VALUES (1);

SELECT * FROM t_point_in_polygon WHERE pointInPolygon(p, [(0, 0), (10, 0), (10, 10), (0, 10)]);

DROP TABLE t_point_in_polygon;

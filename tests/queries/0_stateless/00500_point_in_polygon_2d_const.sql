SELECT pointInPolygon((0, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)]]);

DROP TABLE IF EXISTS s;
CREATE TABLE s (`id` String, `lng` Int64, `lat` Int64) ENGINE = Memory();

DROP TABLE IF EXISTS p;
CREATE TABLE p (`polygon_id` Int64, `polygon_name` String, `shape` Array(Array(Tuple(Float64, Float64))), `state` String) ENGINE = Memory();

INSERT INTO s VALUES ('a', 0, 0);
INSERT INTO p VALUES (8, 'a', [[(0, 0), (10, 0), (10, 10), (0, 10)]], 'a');
SELECT id FROM s WHERE pointInPolygon((lng,lat), (select shape from p where polygon_id = 8));

DROP TABLE s;
DROP TABLE p;

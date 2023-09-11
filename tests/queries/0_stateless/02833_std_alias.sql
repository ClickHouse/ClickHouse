DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x Float64, y Float64) ENGINE = Memory;
INSERT INTO series(i, x, y) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);

SELECT std(x), std(y) FROM series;
SELECT stddevPop(x), stddevPop(y) FROM series;

DROP TABLE series;

USE test;

DROP TABLE IF EXISTS table1;

CREATE TABLE table1 (lat Float64, lon Float64, resolution UInt8) ENGINE = Memory;

INSERT INTO table1 VALUES(55.77922738, 37.63098076, 15);
INSERT INTO table1 VALUES(55.76324100, 37.66018300, 15);
INSERT INTO table1 VALUES(55.72076200, 37.59813500, 15);
INSERT INTO table1 VALUES(55.72076201, 37.59813500, 15);
INSERT INTO table1 VALUES(55.72076200, 37.59813500, 14);

select geoToH3(55.77922738, 37.63098076, 15);
select geoToH3(lat, lon, resolution) from table1 order by lat, lon, resolution;
select geoToH3(lat, lon, 15) from table1 order by lat, lon, geoToH3(lat, lon, 15);
select lat, lon, geoToH3(lat, lon, 15) from table1 order by lat, lon, geoToH3(lat, lon, 15);
select geoToH3(lat, lon, resolution), count(*) from table1 group by geoToH3(lat, lon, resolution) order by geoToH3(lat, lon, resolution);

DROP TABLE table1

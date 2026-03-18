-- Tags: no-fasttest

DROP TABLE IF EXISTS table1;

CREATE TABLE table1 (lat Float64, lon Float64, resolution UInt8) ENGINE = Memory;

INSERT INTO table1 VALUES(55.77922738, 37.63098076, 15);
INSERT INTO table1 VALUES(55.76324100, 37.66018300, 15);
INSERT INTO table1 VALUES(55.72076200, 37.59813500, 15);
INSERT INTO table1 VALUES(55.72076201, 37.59813500, 15);
INSERT INTO table1 VALUES(55.72076200, 37.59813500, 14);

select geoToH3(37.63098076, 55.77922738, 15);
select geoToH3(37.63098076, 55.77922738, 24); -- { serverError ARGUMENT_OUT_OF_BOUND }
select geoToH3(lon, lat, resolution) from table1 order by lat, lon, resolution;
select geoToH3(lon, lat, resolution) AS k from table1 order by lat, lon, k;
select lat, lon, geoToH3(lon, lat, resolution) AS k from table1 order by lat, lon, k;
select geoToH3(lon, lat, resolution) AS k, count(*) from table1 group by k order by k;

DROP TABLE table1

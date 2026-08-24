-- Tags: no-fasttest

select h3ToParent(641573946153969375, 1);
select h3ToParent(641573946153969375, arrayJoin([1,2]));

DROP TABLE IF EXISTS data_table;

CREATE TABLE data_table (id UInt64, longitude Float64, latitude Float64) ENGINE=MergeTree ORDER BY id;
INSERT INTO data_table SELECT number, number, number FROM numbers(10);
SELECT geoToH3(longitude,  latitude, toUInt8(8)) AS h3Index FROM data_table ORDER BY 1;
SELECT geoToH3(longitude,  latitude, toUInt8(longitude - longitude + 8)) AS h3Index FROM data_table ORDER BY 1;

DROP TABLE data_table;

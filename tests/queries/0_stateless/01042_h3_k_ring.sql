-- Tags: no-fasttest

SELECT arraySort(h3kRing(581276613233082367, 1));
SELECT h3kRing(581276613233082367, 0);
SELECT h3kRing(581276613233082367, -1); -- { serverError 12 }


DROP TABLE IF EXISTS h3_indexes;

CREATE TABLE h3_indexes (h3_index UInt64, res UInt8) ENGINE = Memory;

-- Random geo coordinates were generated using the H3 tool: https://github.com/ClickHouse-Extras/h3/blob/master/src/apps/testapps/mkRandGeo.c at various resolutions from 0 to 15.
-- Corresponding H3 index values were in turn generated with those geo coordinates using `geoToH3(lon, lat, res)` ClickHouse function for the following test.

INSERT INTO h3_indexes VALUES (579205133326352383,1);
INSERT INTO h3_indexes VALUES (581263419093549055,2);
INSERT INTO h3_indexes VALUES (589753847883235327,3);
INSERT INTO h3_indexes VALUES (594082350283882495,4);
INSERT INTO h3_indexes VALUES (598372386957426687,5);
INSERT INTO h3_indexes VALUES (599542359671177215,6);
INSERT INTO h3_indexes VALUES (604296355086598143,7);
INSERT INTO h3_indexes VALUES (608785214872748031,8);
INSERT INTO h3_indexes VALUES (615732192485572607,9);
INSERT INTO h3_indexes VALUES (617056794467368959,10);
INSERT INTO h3_indexes VALUES (624586477873168383,11);
INSERT INTO h3_indexes VALUES (627882919484481535,12);
INSERT INTO h3_indexes VALUES (634600058503392255,13);
INSERT INTO h3_indexes VALUES (635544851677385791,14);
INSERT INTO h3_indexes VALUES (639763125756281263,15);
INSERT INTO h3_indexes VALUES (644178757620501158,16);


SELECT arraySort(h3kRing(h3_index, res)) FROM h3_indexes ORDER BY h3_index;

DROP TABLE h3_indexes;

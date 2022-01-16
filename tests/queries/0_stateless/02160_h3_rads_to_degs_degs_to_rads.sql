-- Tags: no-fasttest

DROP TABLE IF EXISTS h3_indexes;


CREATE TABLE h3_indexes (degrees Float64) ENGINE = Memory;


INSERT INTO h3_indexes VALUES (-1);
INSERT INTO h3_indexes VALUES (-180);
INSERT INTO h3_indexes VALUES (-180.6);
INSERT INTO h3_indexes VALUES (-360);
INSERT INTO h3_indexes VALUES (0);
INSERT INTO h3_indexes VALUES (1);
INSERT INTO h3_indexes VALUES (180);
INSERT INTO h3_indexes VALUES (180.5);
INSERT INTO h3_indexes VALUES (360);

select h3RadsToDegs(h3DegsToRads(degrees)) from h3_indexes order by degrees;

DROP TABLE h3_indexes;

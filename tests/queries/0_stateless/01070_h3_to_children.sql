-- Tags: no-fasttest

SELECT h3ToChildren(599405990164561919, 16); -- { serverError 69 }

DROP TABLE IF EXISTS h3_indexes;

CREATE TABLE h3_indexes (h3_index UInt64, res UInt8) ENGINE = Memory;

INSERT INTO h3_indexes VALUES (599405990164561919, 3);
INSERT INTO h3_indexes VALUES (599405990164561919, 6);
INSERT INTO h3_indexes VALUES (599405990164561919, 8);


SELECT arraySort(h3ToChildren(h3_index,res)) FROM h3_indexes ORDER BY res;

DROP TABLE h3_indexes;

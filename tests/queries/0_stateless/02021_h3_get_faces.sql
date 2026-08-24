-- Tags: no-fasttest

DROP TABLE IF EXISTS h3_indexes;

CREATE TABLE h3_indexes (h3_index UInt64) ENGINE = Memory;

-- test H3 indexes from: https://github.com/uber/h3-java/blob/master/src/test/java/com/uber/h3core/TestInspection.java#L86

INSERT INTO h3_indexes VALUES (stringToH3('0x85283473fffffffL'));
INSERT INTO h3_indexes VALUES (stringToH3('85283473fffffff'));
INSERT INTO h3_indexes VALUES (stringToH3('0x8167bffffffffffL'));
INSERT INTO h3_indexes VALUES (stringToH3('0x804dfffffffffffL'));

SELECT arraySort(h3GetFaces(h3_index)) FROM h3_indexes ORDER BY h3_index;

DROP TABLE h3_indexes;

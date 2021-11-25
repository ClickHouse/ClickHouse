-- Tags: no-unbundled, no-fasttest

DROP TABLE IF EXISTS h3_indexes;

CREATE TABLE h3_indexes (h3_index UInt64) ENGINE = Memory;

-- test H3 indexes from: https://github.com/uber/h3-java/blob/master/src/test/java/com/uber/h3core/TestInspection.java#L78

INSERT INTO h3_indexes VALUES (stringToH3('8f28308280f18f2'));
INSERT INTO h3_indexes VALUES (stringToH3('0x8f28308280f18f2L'));
INSERT INTO h3_indexes VALUES (stringToH3('821c07fffffffff'));
INSERT INTO h3_indexes VALUES (stringToH3('0x821c07fffffffffL'));

SELECT h3IsPentagon(h3_index) FROM h3_indexes ORDER BY h3_index;

DROP TABLE h3_indexes;

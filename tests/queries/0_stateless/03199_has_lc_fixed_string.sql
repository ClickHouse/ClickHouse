DROP TABLE IF EXISTS 03199_fixedstring_array;
CREATE TABLE 03199_fixedstring_array (arr Array(LowCardinality(FixedString(8)))) ENGINE = Memory;
INSERT INTO 03199_fixedstring_array VALUES (['a', 'b']), (['c', 'd']);

SELECT has(arr, toFixedString(materialize('a'), 1)) FROM 03199_fixedstring_array;

DROP TABLE 03199_fixedstring_array;

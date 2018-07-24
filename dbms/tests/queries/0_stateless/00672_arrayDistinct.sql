USE test;

SELECT arrayDistinct([1, 2, 3]);
SELECT arrayDistinct([1, 2, 3, 2, 2]);
SELECT arrayDistinct([1, 2, NULL, 5, 2, NULL]);

SELECT arrayDistinct(['1212', 'sef', '343r4']);
SELECT arrayDistinct(['1212', 'sef', '343r4', '1212']);
SELECT arrayDistinct(['1212', 'sef', '343r4', NULL, NULL, '232']);

DROP TABLE IF EXISTS arrayDistinct_test;
CREATE TABLE arrayDistinct_test(arr_int Array(UInt8), arr_string Array(String)) ENGINE=Memory;
INSERT INTO arrayDistinct_test values ([1, 2, 3], ['a', 'b', 'c']), ([21, 21, 21, 21], ['123', '123', '123']);

SELECT arrayDistinct(arr_int) FROM arrayDistinct_test;
SELECT arrayDistinct(arr_string) FROM arrayDistinct_test;

DROP TABLE arrayDistinct_test;

SELECT arrayDistinct([['1212'], ['sef'], ['343r4'], ['1212']]);
SELECT arrayDistinct([(1, 2), (1, 3), (1, 2), (1, 2), (1, 2), (1, 5)]);

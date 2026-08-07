SELECT soundex('');
SELECT soundex('12345');
SELECT soundex('341Jons54326ton');
SELECT soundex('A2222222');
SELECT soundex('Fairdale');
SELECT soundex('Faredale');
SELECT soundex('Jon1s2o3n');
SELECT soundex('Jonson');
SELECT soundex('Jonston');
SELECT soundex('M\acDonald22321');
SELECT soundex('MacDonald');
SELECT soundex('S3344mith0000');
SELECT soundex('Smith');

SELECT '---';

-- same input strings but in a table
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (col String) Engine=MergeTree ORDER BY col;
INSERT INTO tab VALUES ('') ('12345') ('341Jons54326ton') ('A2222222') ('Fairdale') ('Faredale') ('Jon1s2o3n') ('Jonson') ('Jonston') ('M\acDonald22321') ('MacDonald') ('S3344mith0000') ('Smith');

SELECT soundex(col) FROM tab;

DROP TABLE tab;

-- negative tests
SELECT soundex(toFixedString('Smith', 5)); -- { serverError ILLEGAL_COLUMN }
SELECT soundex(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

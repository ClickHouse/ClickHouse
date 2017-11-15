SELECT '**** constant-constant comparisons ****';

SELECT 'ab\0c' < 'ab\0d', 'ab\0c' > 'ab\0d';
SELECT 'a' < 'a\0b', 'a' > 'a\0b';
SELECT 'a\0\0\0\0' < 'a\0\0\0', 'a\0\0\0\0' > 'a\0\0\0';

DROP TABLE IF EXISTS test.strings;
CREATE TABLE test.strings(x String, y String) ENGINE = TinyLog;

INSERT INTO test.strings VALUES
    ('abcde\0', 'abcde'), ('aa\0a', 'aa\0b'), ('aa', 'aa\0'), ('a\0\0\0\0', 'a\0\0\0'), ('a\0\0', 'a\0'), ('a', 'a');

SELECT '**** vector-vector comparisons ****';

SELECT x < y, x > y FROM test.strings;

SELECT '**** vector-constant comparisons ****';

SELECT x < 'aa', x > 'aa' FROM test.strings;

SELECT '****';

SELECT x < 'a\0', x > 'a\0' FROM test.strings;

SELECT '**** single-column sort ****'; -- Uses ColumnString::getPermutation()

SELECT * FROM test.strings ORDER BY x;

SELECT '**** multi-column sort ****'; -- Uses ColumnString::compareAt()

SELECT * FROM test.strings ORDER BY x, y;

DROP TABLE test.strings;

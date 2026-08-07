SELECT CAST('hello' AS FixedString(5)) LIKE 'hello';
SELECT CAST('hello' AS FixedString(5)) LIKE 'world';
SELECT CAST('hello' AS FixedString(5)) LIKE 'xyz';
SELECT CAST('hello' AS FixedString(5)) LIKE 'hell';
SELECT CAST('hello' AS FixedString(5)) LIKE 'orld';

SELECT CAST('hello' AS FixedString(5)) LIKE '%hello%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%world%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%xyz%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%hell%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%orld%';

SELECT CAST('hello' AS FixedString(5)) LIKE '%hello';
SELECT CAST('hello' AS FixedString(5)) LIKE '%world';
SELECT CAST('hello' AS FixedString(5)) LIKE '%xyz';
SELECT CAST('hello' AS FixedString(5)) LIKE '%hell';
SELECT CAST('hello' AS FixedString(5)) LIKE '%orld';

SELECT CAST('hello' AS FixedString(5)) LIKE 'hello%';
SELECT CAST('hello' AS FixedString(5)) LIKE 'world%';
SELECT CAST('hello' AS FixedString(5)) LIKE 'xyz%';
SELECT CAST('hello' AS FixedString(5)) LIKE 'hell%';
SELECT CAST('hello' AS FixedString(5)) LIKE 'orld%';

SELECT CAST('hello' AS FixedString(5)) LIKE '%he%o%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%w%ld%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%x%z%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%hell_';
SELECT CAST('hello' AS FixedString(5)) LIKE '_orld%';

SELECT CAST('hello' AS FixedString(5)) LIKE '%he__o%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%w__ld%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%x%z%';
SELECT CAST('hello' AS FixedString(5)) LIKE 'hell_';
SELECT CAST('hello' AS FixedString(5)) LIKE '_orld';

SELECT CAST('hello' AS FixedString(5)) LIKE 'helloworld';
SELECT CAST('hello' AS FixedString(5)) LIKE '%helloworld%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%elloworl%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%ow%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%o%w%';

SELECT CAST('hello' AS FixedString(5)) LIKE '%o%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%l%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%l%o%';
SELECT CAST('hello' AS FixedString(5)) LIKE '%o%l%';

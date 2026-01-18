SELECT '--- short array ASC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a ASC NULLS FIRST;
SELECT '--- short array ASC NULLS LAST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a ASC NULLS LAST;
SELECT '--- short array DESC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a DESC NULLS FIRST;
SELECT '--- short array DESC NULLS LAST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a DESC NULLS LAST;

-- After 256 elements radix sort is used
SELECT '--- long array ASC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a ASC NULLS FIRST;
SELECT '--- long array ASC NULLS LAST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a ASC NULLS LAST;
SELECT '--- long array DESC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a DESC NULLS FIRST;
SELECT '--- long array DESC NULLS LAST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a DESC NULLS LAST;

-- Same for sort of ranges

SELECT '--- short array partial ASC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a ASC NULLS FIRST, 1 ASC;
SELECT '--- short array partial ASC NULLS LAST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a ASC NULLS LAST, 1 ASC;
SELECT '--- short array partial DESC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a DESC NULLS FIRST, 1 ASC;
SELECT '--- short array partial DESC NULLS LAST';
SELECT number + number / number AS a FROM numbers(3) ORDER BY a DESC NULLS LAST, 1 ASC;

-- After 256 elements radix sort is used
SELECT '--- long array partial ASC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a ASC NULLS FIRST, 1 ASC;
SELECT '--- long array partial ASC NULLS LAST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a ASC NULLS LAST, 1 ASC;
SELECT '--- long array partial DESC NULLS FIRST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a DESC NULLS FIRST, 1 ASC;
SELECT '--- long array partial DESC NULLS LAST';
SELECT number + number / number AS a FROM numbers(256) ORDER BY a DESC NULLS LAST, 1 ASC;

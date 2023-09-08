SELECT NOT x, isZeroOrNull(x) FROM (SELECT arrayJoin([1, 2, 3, NULL]) = 3 AS x);
SELECT '---';
SELECT NOT x, isZeroOrNull(x) FROM (SELECT arrayJoin([1, 2, 3]) = 3 AS x);
SELECT '---';
CREATE TEMPORARY TABLE test (x String NULL);
INSERT INTO test VALUES ('hello'), ('world'), ('xyz'), (NULL);

SELECT * FROM test WHERE x != 'xyz';
SELECT '---';
SELECT * FROM test WHERE NOT x = 'xyz';
SELECT '---';
SELECT * FROM test WHERE isZeroOrNull(x = 'xyz');
SELECT '---';

SELECT count() FROM
(
    SELECT * FROM test WHERE x != 'xyz'
    UNION ALL
    SELECT * FROM test WHERE NOT x != 'xyz'
);

SELECT '---';

SELECT count() FROM
(
    SELECT * FROM test WHERE x != 'xyz'
    UNION ALL
    SELECT * FROM test WHERE isZeroOrNull(x != 'xyz')
);

SELECT '---';

select isZeroOrNull(Null);

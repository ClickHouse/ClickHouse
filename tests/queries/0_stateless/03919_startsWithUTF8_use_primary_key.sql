DROP TABLE IF EXISTS test_startsWithUTF8;
CREATE TABLE test_startsWithUTF8 (a String) Engine = MergeTree() ORDER BY a SETTINGS index_granularity = 1;

INSERT INTO test_startsWithUTF8 (a) VALUES ('a'), ('abcd'), ('bbb'), (''), ('abc');

SELECT count() from test_startsWithUTF8
WHERE startsWithUTF8(a, 'a')
SETTINGS force_primary_key=1;

SELECT count() from test_startsWithUTF8
WHERE startsWithUTF8(a, '🙂')
SETTINGS force_primary_key=1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM test_startsWithUTF8
WHERE startsWithUTF8('a', a)
SETTINGS force_primary_key=1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM test_startsWithUTF8
WHERE startsWithUTF8('a', a);

SELECT count() FROM test_startsWithUTF8
WHERE startsWithUTF8('a', a)
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM test_startsWithUTF8
WHERE startsWithUTF8(a, '')
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM test_startsWithUTF8
WHERE startsWithUTF8(a, substring(a, 1, 1))
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM test_startsWithUTF8
WHERE startsWithUTF8(a, concat('a', ''))
SETTINGS force_primary_key = 1;

SELECT *
FROM test_startsWithUTF8
WHERE startsWithUTF8(a, 'a')
ORDER BY a
SETTINGS
    force_primary_key = 1,
    max_rows_to_read = 4,
    read_overflow_mode = 'throw';

DROP TABLE test_startsWithUTF8;



DROP TABLE IF EXISTS test_startsWithUTF8_invalid;
CREATE TABLE test_startsWithUTF8_invalid
(
    a String
) ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 1;

INSERT INTO test_startsWithUTF8_invalid
SELECT arrayJoin([
    'a', 'abcd', 'bbb', '', 'abc',
    unhex('80'),        -- invalid UTF-8
    unhex('F0808080')   -- invalid UTF-8
]);

SELECT count()
FROM test_startsWithUTF8_invalid
WHERE startsWithUTF8(a, 'a')
SETTINGS force_primary_key = 1;

SELECT count()
FROM test_startsWithUTF8_invalid
WHERE startsWithUTF8(a, '🙂')
SETTINGS force_primary_key = 1;  -- { serverError INDEX_NOT_USED }

DROP TABLE test_startsWithUTF8_invalid;

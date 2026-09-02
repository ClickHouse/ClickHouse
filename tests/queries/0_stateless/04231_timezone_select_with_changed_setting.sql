SET session_timezone = 'UTC';

CREATE TABLE test_tz (d DateTime) ENGINE=Memory AS
SELECT toDateTime('2000-01-01 00:00:00', 'UTC');

SELECT *, timezone()
FROM test_tz
WHERE d = toDateTime('2000-01-01 00:00:00', 'UTC')
SETTINGS session_timezone = 'Asia/Novosibirsk'
FORMAT TSV;

SELECT '---';

SELECT *, timezone()
FROM test_tz
WHERE d = '2000-01-01 00:00:00'
SETTINGS session_timezone = 'Asia/Novosibirsk'
FORMAT TSV;

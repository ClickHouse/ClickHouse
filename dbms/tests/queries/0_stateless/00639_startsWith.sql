USE test;

SELECT startsWith(s, 'He') FROM (SELECT arrayJoin(['', 'H', 'He', 'Hellow', '3434', 'fffffffffdHe']) AS s);
SELECT startsWith(s, '') FROM (SELECT arrayJoin(['', 'h', 'hi']) AS s);
SELECT startsWith('123', '123');
SELECT startsWith('123', '12');
SELECT startsWith('123', '1234');
SELECT startsWith('123', '');

DROP TABLE IF EXISTS startsWith_test;
CREATE TABLE startsWith_test(S1 String, S2 String, S3 FixedString(2)) ENGINE=Memory;
INSERT INTO startsWith_test values ('11', '22', '33'), ('a', 'a', 'bb'), ('abc', 'ab', '23');

SELECT COUNT() FROM startsWith_test WHERE startsWith(S1, S1);
SELECT COUNT() FROM startsWith_test WHERE startsWith(S1, S2);
SELECT COUNT() FROM startsWith_test WHERE startsWith(S2, S3);
DROP TABLE startsWith_test;

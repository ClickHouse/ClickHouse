SELECT endsWith(s, 'ow') FROM (SELECT arrayJoin(['', 'o', 'ow', 'Hellow', '3434', 'owfffffffdHe']) AS s);
SELECT endsWith(s, '') FROM (SELECT arrayJoin(['', 'h', 'hi']) AS s);
SELECT endsWith('123', '3');
SELECT endsWith('123', '23');
SELECT endsWith('123', '32');
SELECT endsWith('123', '');

DROP TABLE IF EXISTS endsWith_test;
CREATE TABLE endsWith_test(S1 String, S2 String, S3 FixedString(2)) ENGINE=Memory;
INSERT INTO endsWith_test values ('11', '22', '33'), ('a', 'a', 'bb'), ('abc', 'bc', '23');

SELECT COUNT() FROM endsWith_test WHERE endsWith(S1, S1);
SELECT COUNT() FROM endsWith_test WHERE endsWith(S1, S2);
SELECT COUNT() FROM endsWith_test WHERE endsWith(S2, S3);

SELECT endsWith([], 'str'); -- { serverError 43 }
DROP TABLE endsWith_test;

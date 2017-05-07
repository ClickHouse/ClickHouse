DROP TABLE IF EXISTS test.replaceall;
CREATE TABLE test.replaceall (str FixedString(3)) ENGINE = Memory;

INSERT INTO test.replaceall VALUES ('foo');
INSERT INTO test.replaceall VALUES ('boa');
INSERT INTO test.replaceall VALUES ('bar');
INSERT INTO test.replaceall VALUES ('bao');

SELECT
    str,
    replaceAll(str, 'o', '*') AS replaced
FROM test.replaceall
ORDER BY str ASC;

DROP TABLE test.replaceall;

CREATE TABLE test.replaceall (date Date DEFAULT today(), fs FixedString(16)) ENGINE = MergeTree(date, (date, fs), 8192);
INSERT INTO test.replaceall (fs) VALUES ('54db0d43009d\0\0\0\0'), ('fe2b58224766cf10'), ('54db0d43009d\0\0\0\0'), ('fe2b58224766cf10');

SELECT fs, replaceAll(fs, '\0', '*')
FROM test.replaceall
ORDER BY fs ASC;

DROP TABLE test.replaceall;

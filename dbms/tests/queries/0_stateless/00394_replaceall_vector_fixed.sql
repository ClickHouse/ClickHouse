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

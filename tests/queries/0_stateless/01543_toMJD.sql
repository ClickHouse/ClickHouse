--
SELECT 'Invocation with constant';

SELECT toMJD('1858-11-16');
SELECT toMJD('1858-11-17');
SELECT toMJD('2020-11-01');
SELECT toMJD(NULL);
SELECT toMJD('unparsable'); -- { serverError 27 }
SELECT toMJD('1999-02-29'); -- { serverError 38 }
SELECT toMJD('1999-13-32'); -- { serverError 38 }

SELECT 'or null';
SELECT toMJDOrNull('2020-11-01');
SELECT toMJDOrNull('unparsable');
SELECT toMJDOrNull('1999-02-29');
SELECT toMJDOrNull('1999-13-32');

--
SELECT 'Invocation with String column';

DROP TABLE IF EXISTS toMJD_test;
CREATE TABLE toMJD_test (d String) ENGINE = Memory;

INSERT INTO toMJD_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT toMJD(d) FROM toMJD_test;

DROP TABLE toMJD_test;

--
SELECT 'Invocation with FixedString column';

DROP TABLE IF EXISTS toMJD_test;
CREATE TABLE toMJD_test (d FixedString(10)) ENGINE = Memory;

INSERT INTO toMJD_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT toMJD(d) FROM toMJD_test;

DROP TABLE toMJD_test;

--
SELECT 'Invocation with constant';

SELECT toModifiedJulianDay('1858-11-16');
SELECT toModifiedJulianDay('1858-11-17');
SELECT toModifiedJulianDay('2020-11-01');
SELECT toModifiedJulianDay(NULL);
SELECT toModifiedJulianDay('unparsable'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT toModifiedJulianDay('1999-02-29'); -- { serverError CANNOT_PARSE_DATE }
SELECT toModifiedJulianDay('1999-13-32'); -- { serverError CANNOT_PARSE_DATE }

SELECT 'or null';
SELECT toModifiedJulianDayOrNull('2020-11-01');
SELECT toModifiedJulianDayOrNull('unparsable');
SELECT toModifiedJulianDayOrNull('1999-02-29');
SELECT toModifiedJulianDayOrNull('1999-13-32');

--
SELECT 'Invocation with String column';

DROP TABLE IF EXISTS toModifiedJulianDay_test;
CREATE TABLE toModifiedJulianDay_test (d String) ENGINE = Memory;

INSERT INTO toModifiedJulianDay_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT toModifiedJulianDay(d) FROM toModifiedJulianDay_test;

DROP TABLE toModifiedJulianDay_test;

--
SELECT 'Invocation with FixedString column';

DROP TABLE IF EXISTS toModifiedJulianDay_test;
CREATE TABLE toModifiedJulianDay_test (d FixedString(10)) ENGINE = Memory;

INSERT INTO toModifiedJulianDay_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT toModifiedJulianDay(d) FROM toModifiedJulianDay_test;

DROP TABLE toModifiedJulianDay_test;

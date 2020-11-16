--
SELECT 'Invocation with constant';

SELECT fromMJD(-1);
SELECT fromMJD(0);
SELECT fromMJD(59154);
SELECT fromMJD(NULL);
SELECT fromMJD(-678942); -- { serverError 490 }
SELECT fromMJD(2973484); -- { serverError 490 }

SELECT 'or null';
SELECT fromMJDOrNull(59154);
SELECT fromMJDOrNull(NULL);
SELECT fromMJDOrNull(-678942);
SELECT fromMJDOrNull(2973484);

--
SELECT 'Invocation with Int32 column';

DROP TABLE IF EXISTS fromMJD_test;
CREATE TABLE fromMJD_test (d Int32) ENGINE = Memory;

INSERT INTO fromMJD_test VALUES (-1), (0), (59154);
SELECT fromMJD(d) FROM fromMJD_test;

DROP TABLE fromMJD_test;

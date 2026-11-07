SET session_timezone = 'UTC';
CREATE TABLE json_session_timezone
(
    j JSON(d DateTime, n Array(DateTime64(3)), fixed DateTime('UTC'), nested JSON(d DateTime))
) ENGINE = Memory;

INSERT INTO json_session_timezone VALUES
('{"d":"2024-01-01 12:00:00","n":["2024-01-01 12:00:00.123"],"fixed":"2024-01-01 12:00:00","nested":{"d":"2024-01-01 12:00:00"}}');

SELECT j FROM json_session_timezone;
SET session_timezone = 'Asia/Tokyo';
SELECT j FROM json_session_timezone;
SELECT j.d, j.n, j.fixed, j.nested.d FROM json_session_timezone;
SET session_timezone = 'Europe/Amsterdam';
SELECT j FROM json_session_timezone;
SET session_timezone = 'UTC';
SELECT j FROM json_session_timezone;

DROP TABLE json_session_timezone;

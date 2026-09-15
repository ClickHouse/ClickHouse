-- Typed `Date` and `Date32` values, including wrapped types, must keep their calendar dates.
SET session_timezone = 'UTC';
CREATE TABLE json_date_timezone
(
    j JSON(d Date, d32 Date32),
    wrapped JSON(d Array(Nullable(Date)), d32 Tuple(v Date32))
) ENGINE = Memory;
INSERT INTO json_date_timezone VALUES
('{"d":"2011-12-30","d32":"1969-12-31"}', '{"d":["2011-12-30",null],"d32":{"v":"1969-12-31"}}');
SELECT j, wrapped FROM json_date_timezone;
SET session_timezone = 'Pacific/Apia';
SELECT j, wrapped FROM json_date_timezone;
SELECT j, wrapped FROM json_date_timezone FORMAT JSONEachRow;
SET session_timezone = 'Asia/Tokyo';
SELECT j, wrapped FROM json_date_timezone;
SET session_timezone = 'America/Los_Angeles';
SELECT j, wrapped FROM json_date_timezone;
SET session_timezone = 'UTC';
SELECT j, wrapped FROM json_date_timezone;
DROP TABLE json_date_timezone;

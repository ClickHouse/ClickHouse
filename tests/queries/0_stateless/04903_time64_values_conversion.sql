SET allow_experimental_time_time64_type = 1;
SET session_timezone = 'America/New_York';

CREATE TABLE time64_values_conversion
(
    datetime64 DateTime64(3, 'UTC'),
    datetime DateTime('UTC'),
    date Date,
    date32 Date32,
    time_date Date
) ENGINE = Memory;

INSERT INTO time64_values_conversion VALUES
(
    toTime64('01:02:03.456', 3),
    toTime64('01:02:03.456', 3),
    toTime64('01:02:03.456', 3),
    toTime64('01:02:03.456', 3),
    toTime('01:02:03')
);

SELECT * FROM time64_values_conversion FORMAT TSV;

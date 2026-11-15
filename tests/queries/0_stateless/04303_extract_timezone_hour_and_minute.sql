-- Tests EXTRACT(TIMEZONE_HOUR FROM ...) and EXTRACT(TIMEZONE_MINUTE FROM ...) for PostgreSQL compatibility.
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/105956

-- Whole-hour positive offset (UTC+1 in winter).
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Europe/Berlin'));
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Europe/Berlin'));

-- Whole-hour negative offset (UTC-5 in EST).
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'America/New_York'));
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'America/New_York'));

-- Half-hour positive offset (UTC+5:30, no DST).
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));

-- 45-minute positive offset (UTC+5:45, no DST).
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kathmandu'));
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kathmandu'));

-- Half-hour negative offset (UTC-3:30 in NST).
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'America/St_Johns'));
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'America/St_Johns'));

-- UTC.
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'UTC'));
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'UTC'));

-- Lowercase keywords.
SELECT EXTRACT(timezone_hour   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));
SELECT EXTRACT(timezone_minute FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));

-- `date_part` form.
SELECT date_part('timezone_hour',   toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));
SELECT date_part('timezone_minute', toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));
SELECT date_part('TIMEZONE_HOUR',   toDateTime('2024-01-15 12:00:00', 'America/St_Johns'));
SELECT date_part('TIMEZONE_MINUTE', toDateTime('2024-01-15 12:00:00', 'America/St_Johns'));

-- DateTime64 input.
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime64('2024-01-15 12:00:00.123', 3, 'Asia/Kolkata'));
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime64('2024-01-15 12:00:00.123', 3, 'Asia/Kolkata'));

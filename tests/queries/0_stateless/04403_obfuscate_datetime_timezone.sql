-- `DateTime` columns can carry an explicit timezone that differs from the server timezone. The
-- obfuscator preserves the date component, which is the date as *displayed* in the column's
-- timezone, so `DateTimeModel` must use the column's timezone (via `DataTypeDateTime::getTimeZone`)
-- rather than the server timezone. Otherwise values near local midnight move to a neighbouring date.
--
-- All inputs below are on the Tokyo date 2024-01-01 (00:00:00 .. 00:16:39 'Asia/Tokyo'), which maps
-- to the UTC date 2023-12-31. With the server timezone (UTC) the preserved date would be 2023-12-31
-- and many obfuscated values would display on a neighbouring Tokyo date; with the column timezone
-- every obfuscated value keeps the Tokyo date 2024-01-01.

SELECT DISTINCT toDate(x, 'Asia/Tokyo')
FROM (
    SELECT * FROM obfuscate(
        SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo') + toIntervalSecond(number) AS x
        FROM numbers(1000))
    SETTINGS obfuscate_seed = 'obfuscate_datetime_timezone');

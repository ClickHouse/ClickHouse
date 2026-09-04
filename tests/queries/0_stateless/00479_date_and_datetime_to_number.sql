SELECT toYYYYMM(toDate('2017-07-21'));
SELECT toYYYYMMDD(toDate('2017-07-21'));
SELECT toYYYYMMDDhhmmss(toDate('2017-07-21'));
SELECT toYYYYMM(toDateTime('2017-07-21T11:22:33'));
SELECT toYYYYMMDD(toDateTime('2017-07-21T11:22:33'));
SELECT toYYYYMMDDhhmmss(toDateTime('2017-07-21T11:22:33'));

-- The extremes of the two types: a `DateTime` is a `UInt32` and a `Date` is a `UInt16` day number,
-- so neither can denote an instant outside the calendar lookup table.
SELECT toYYYYMMDDhhmmss(toDateTime(0, 'UTC'), 'UTC');
SELECT toYYYYMMDDhhmmss(toDateTime(4294967295, 'UTC'), 'UTC');
SELECT toYYYYMMDDhhmmss(toDate('1970-01-01'), 'UTC');
SELECT toYYYYMMDDhhmmss(toDate(65535), 'UTC');

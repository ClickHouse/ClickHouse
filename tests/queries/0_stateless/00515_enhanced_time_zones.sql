SELECT addMonths(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 1, 'Asia/Kolkata');
SELECT addMonths(toDateTime('2017-11-05 10:37:47', 'Asia/Kolkata'), 1);
SELECT addMonths(toTimeZone(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata'), 1);

SELECT addMonths(toDateTime('2017-11-05 08:07:47'), 1);
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), 1);
SELECT addMonths(toDateTime('2017-11-05 08:07:47'), materialize(1));
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), materialize(1));

SELECT addMonths(toDateTime('2017-11-05 08:07:47'), -1);
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), -1);
SELECT addMonths(toDateTime('2017-11-05 08:07:47'), materialize(-1));
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), materialize(-1));

SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Istanbul');
SELECT toUnixTimestamp(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Istanbul');

SELECT toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul');
SELECT toTimeZone(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata');
SELECT toString(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'));
SELECT toString(toTimeZone(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata'));
SELECT toString(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata');

SELECT toTimeZone(dt, tz) FROM (
    SELECT toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul') AS dt, arrayJoin(['Asia/Kolkata', 'UTC']) AS tz
); -- { serverError ILLEGAL_COLUMN }
SELECT materialize('Asia/Kolkata') t, toTimeZone(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), t); -- { serverError ILLEGAL_COLUMN }

CREATE TEMPORARY TABLE tmp AS SELECT arrayJoin(['Europe/Istanbul', 'Asia/Istanbul']);
SELECT toTimeZone(now(), (*,).1) FROM tmp; -- { serverError ILLEGAL_COLUMN }
SELECT now((*,).1) FROM tmp; -- { serverError ILLEGAL_COLUMN }
SELECT now64(1, (*,).1) FROM tmp; -- { serverError ILLEGAL_COLUMN }
SELECT toStartOfInterval(now(), INTERVAL 3 HOUR, (*,).1) FROM tmp; -- { serverError ILLEGAL_COLUMN }
SELECT snowflakeToDateTime(toInt64(123), (*,).1) FROM tmp; -- { serverError ILLEGAL_COLUMN }
SELECT toUnixTimestamp(now(), (*,).1) FROM tmp; -- { serverError ILLEGAL_COLUMN }
SELECT toDateTimeOrDefault('2023-04-12 16:43:32', (*,).1, now()) FROM tmp; -- { serverError ILLEGAL_COLUMN }

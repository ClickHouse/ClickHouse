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

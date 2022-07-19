SELECT addMonths(toDateTime('2017-11-05 08:07:47', 'Europe/Moscow'), 1, 'Asia/Kolkata');
SELECT addMonths(toDateTime('2017-11-05 10:37:47', 'Asia/Kolkata'), 1);
SELECT addMonths(toTimeZone(toDateTime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata'), 1);

SELECT addMonths(toDateTime('2017-11-05 08:07:47'), 1);
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), 1);
SELECT addMonths(toDateTime('2017-11-05 08:07:47'), materialize(1));
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), materialize(1));

SELECT addMonths(toDateTime('2017-11-05 08:07:47'), -1);
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), -1);
SELECT addMonths(toDateTime('2017-11-05 08:07:47'), materialize(-1));
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), materialize(-1));

SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Europe/Moscow');
SELECT toUnixTimestamp(toDateTime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Europe/Moscow');

SELECT toDateTime('2017-11-05 08:07:47', 'Europe/Moscow');
SELECT toTimeZone(toDateTime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata');
SELECT toString(toDateTime('2017-11-05 08:07:47', 'Europe/Moscow'));
SELECT toString(toTimeZone(toDateTime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata'));
SELECT toString(toDateTime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata');

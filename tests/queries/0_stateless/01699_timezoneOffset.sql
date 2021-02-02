
/* timestamp  ==  (Europe/Moscow) */


SELECT number,(toDateTime('1981-04-01 00:00:00', 'Europe/Moscow') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, toUnixTimestamp(k) as s FROM numbers(200);
SELECT number,(toDateTime('1981-09-30 00:00:00', 'Europe/Moscow') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, toUnixTimestamp(k) as s FROM numbers(200);


SELECT number,(toDateTime('2020-03-21 00:00:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, toUnixTimestamp(k) as s FROM numbers(200);

SELECT number,(toDateTime('2020-09-20 00:00:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, toUnixTimestamp(k) as s FROM numbers(200);

SELECT timezoneOffset(toDateTime('2018-08-21 22:20:00', 'Australia/Lord_Howe'));
SELECT timezoneOffset(toDateTime('2018-02-21 22:20:00', 'Australia/Lord_Howe'));


SELECT number,(toDateTime('2020-10-03 00:00:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, toUnixTimestamp(k) as s FROM numbers(200);

SELECT number,(toDateTime('2019-04-06 00:00:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, toUnixTimestamp(k) as s FROM numbers(200);


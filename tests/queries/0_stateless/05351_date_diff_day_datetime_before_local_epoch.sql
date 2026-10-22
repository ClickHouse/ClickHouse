-- A DateTime on the local day 1969-12-31 (west of UTC) is day -1 for dateDiff and age with unit day, as for DateTime64.

SELECT dateDiff('day', toDateTime(0, 'America/New_York'), toDateTime(86400, 'America/New_York'));
SELECT age('day', toDateTime(0, 'America/New_York'), toDateTime(172800, 'America/New_York'));
SELECT dateDiff('day', toDateTime(86400, 'America/New_York'), toDateTime(0, 'America/New_York'));
SELECT age('day', toDateTime(172800, 'America/New_York'), toDateTime(0, 'America/New_York'));
SELECT dateDiff('day', toDateTime(0, 'UTC'), toDateTime(86400, 'UTC'), 'America/New_York');
SELECT dateDiff('day', toDateTime(0, 'America/New_York'), toDate('1970-01-10'));
SELECT dateDiff('day', toDate32('1969-12-30'), toDateTime(0, 'America/New_York'));
SELECT groupArray(dateDiff('day', a, toDateTime(86400, 'America/New_York'))), groupArray(age('day', a, toDateTime(172800, 'America/New_York')))
FROM (SELECT toDateTime(number * 3600, 'America/New_York') AS a FROM numbers(7));

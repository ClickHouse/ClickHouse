-- { echo }

SELECT tumbleStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa');
SELECT toDateTime(tumbleStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(tumbleStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT tumbleStart(tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa'));
SELECT tumbleEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa');
SELECT toDateTime(tumbleEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(tumbleEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT tumbleEnd(tumble(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, 'US/Samoa'));

SELECT hopStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa');
SELECT toDateTime(hopStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(hopStart(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT hopStart(hop(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa'));
SELECT hopEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa');
SELECT toDateTime(hopEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT toDateTime(hopEnd(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa'), 'US/Samoa');
SELECT hopEnd(hop(toDateTime('2019-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' WEEK, INTERVAL '3' WEEK, 'US/Samoa'));

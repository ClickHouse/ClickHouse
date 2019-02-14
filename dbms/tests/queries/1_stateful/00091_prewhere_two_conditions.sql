SET max_bytes_to_read = 200000000;

SET optimize_move_to_prewhere = 1;

SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND EventTime < '2014-03-21 00:00:00';
SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND URL != '' AND EventTime < '2014-03-21 00:00:00';

SET optimize_move_to_prewhere = 0;

SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND EventTime < '2014-03-21 00:00:00'; -- { serverError 307 }
SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND URL != '' AND EventTime < '2014-03-21 00:00:00'; -- { serverError 307 }

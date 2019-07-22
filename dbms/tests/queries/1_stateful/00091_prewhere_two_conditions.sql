SET max_bytes_to_read = 200000000;

SET optimize_move_to_prewhere = 1;

SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND EventTime < '2014-03-21 00:00:00';
SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND URL != '' AND EventTime < '2014-03-21 00:00:00';
SELECT uniq(*) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND EventTime < '2014-03-21 00:00:00' AND EventDate = '2014-03-21';
WITH EventTime AS xyz SELECT uniq(*) FROM test.hits WHERE xyz >= '2014-03-20 00:00:00' AND xyz < '2014-03-21 00:00:00' AND EventDate = '2014-03-21';

SET optimize_move_to_prewhere = 0;

SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND EventTime < '2014-03-21 00:00:00'; -- { serverError 307 }
SELECT uniq(URL) FROM test.hits WHERE EventTime >= '2014-03-20 00:00:00' AND URL != '' AND EventTime < '2014-03-21 00:00:00'; -- { serverError 307 }

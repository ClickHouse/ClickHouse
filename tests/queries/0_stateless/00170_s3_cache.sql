-- Tags: stateful, no-parallel, no-random-settings
-- no-parallel: Heavy and it drops filesystem cache

-- { echo }

SET allow_prefetched_read_pool_for_remote_filesystem=0;
SET enable_filesystem_cache_on_write_operations=0;
SET max_memory_usage='20G';
SET read_through_distributed_cache = 1;
SYSTEM DROP FILESYSTEM CACHE;
SELECT count() FROM test.hits_s3;
SELECT count() FROM test.hits_s3 WHERE AdvEngineID != 0;
SELECT sum(AdvEngineID), count(), avg(ResolutionWidth) FROM test.hits_s3 ;
SELECT sum(UserID) FROM test.hits_s3 ;
SELECT uniq(UserID) FROM test.hits_s3 ;
SELECT uniq(SearchPhrase) FROM test.hits_s3 ;
SELECT min(EventDate), max(EventDate) FROM test.hits_s3 ;
SELECT AdvEngineID, count() FROM test.hits_s3 WHERE AdvEngineID != 0 GROUP BY AdvEngineID ORDER BY AdvEngineID DESC;
SELECT RegionID, uniq(UserID) AS u FROM test.hits_s3 GROUP BY RegionID ORDER BY u DESC LIMIT 10;
SELECT RegionID, sum(AdvEngineID), count() AS c, avg(ResolutionWidth), uniq(UserID) FROM test.hits_s3 GROUP BY RegionID ORDER BY c DESC LIMIT 10;
SELECT MobilePhoneModel, uniq(UserID) AS u FROM test.hits_s3 WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
SELECT MobilePhone, MobilePhoneModel, uniq(UserID) AS u FROM test.hits_s3 WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
SELECT uniq(SearchPhrase), count() AS c FROM test.hits_s3 WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
SELECT uniq(SearchPhrase), uniq(UserID) AS u FROM test.hits_s3 WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
SELECT SearchEngineID, uniq(SearchPhrase), count() AS c FROM test.hits_s3 WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
SELECT UserID, count() FROM test.hits_s3 GROUP BY UserID ORDER BY count() DESC LIMIT 10;
SELECT UserID, uniq(SearchPhrase) as m, count() as c FROM test.hits_s3 GROUP BY UserID, SearchPhrase ORDER BY UserID, m, c DESC LIMIT 10;
SELECT UserID, uniq(SearchPhrase) as m, count() as c FROM test.hits_s3 GROUP BY UserID, SearchPhrase ORDER BY UserID, m, c LIMIT 10;
SELECT UserID, toMinute(EventTime) AS m, uniq(SearchPhrase) as u, count() as c FROM test.hits_s3 GROUP BY UserID, m, SearchPhrase ORDER BY UserID DESC LIMIT 10 FORMAT Null;
SELECT UserID FROM test.hits_s3 WHERE UserID = 12345678901234567890;
SELECT count() FROM test.hits_s3 WHERE URL LIKE '%metrika%';
SELECT uniq(SearchPhrase) as u, max(URL) as m, count() AS c FROM test.hits_s3 WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY u, m, c DESC LIMIT 10;
SELECT uniq(SearchPhrase), max(URL), max(Title), count() AS c, uniq(UserID) FROM test.hits_s3 WHERE Title LIKE '%Яндекс%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
SELECT * FROM test.hits_s3 WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10 format Null;
SELECT SearchPhrase FROM test.hits_s3 WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10 FORMAT Null;
SELECT SearchPhrase FROM test.hits_s3 WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10 FORMAT Null;
SELECT SearchPhrase FROM test.hits_s3 WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10 FORMAT Null;
SELECT CounterID, avg(length(URL)) AS l, count() AS c FROM test.hits_s3 WHERE URL != '' GROUP BY CounterID HAVING c > 100000 ORDER BY l DESC LIMIT 25;
SELECT domainWithoutWWW(Referer) AS key, avg(length(Referer)) AS l, count() AS c, max(Referer) FROM test.hits_s3 WHERE Referer != '' GROUP BY key HAVING c > 100000 ORDER BY l DESC LIMIT 25;
SELECT sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 3), sum(ResolutionWidth + 4), sum(ResolutionWidth + 5), sum(ResolutionWidth + 6), sum(ResolutionWidth + 7), sum(ResolutionWidth + 8), sum(ResolutionWidth + 9), sum(ResolutionWidth + 10), sum(ResolutionWidth + 11), sum(ResolutionWidth + 12), sum(ResolutionWidth + 13), sum(ResolutionWidth + 14), sum(ResolutionWidth + 15), sum(ResolutionWidth + 16), sum(ResolutionWidth + 17), sum(ResolutionWidth + 18), sum(ResolutionWidth + 19), sum(ResolutionWidth + 20), sum(ResolutionWidth + 21), sum(ResolutionWidth + 22), sum(ResolutionWidth + 23), sum(ResolutionWidth + 24), sum(ResolutionWidth + 25), sum(ResolutionWidth + 26), sum(ResolutionWidth + 27), sum(ResolutionWidth + 28), sum(ResolutionWidth + 29), sum(ResolutionWidth + 30), sum(ResolutionWidth + 31), sum(ResolutionWidth + 32), sum(ResolutionWidth + 33), sum(ResolutionWidth + 34), sum(ResolutionWidth + 35), sum(ResolutionWidth + 36), sum(ResolutionWidth + 37), sum(ResolutionWidth + 38), sum(ResolutionWidth + 39), sum(ResolutionWidth + 40), sum(ResolutionWidth + 41), sum(ResolutionWidth + 42), sum(ResolutionWidth + 43), sum(ResolutionWidth + 44), sum(ResolutionWidth + 45), sum(ResolutionWidth + 46), sum(ResolutionWidth + 47), sum(ResolutionWidth + 48), sum(ResolutionWidth + 49), sum(ResolutionWidth + 50), sum(ResolutionWidth + 51), sum(ResolutionWidth + 52), sum(ResolutionWidth + 53), sum(ResolutionWidth + 54), sum(ResolutionWidth + 55), sum(ResolutionWidth + 56), sum(ResolutionWidth + 57), sum(ResolutionWidth + 58), sum(ResolutionWidth + 59), sum(ResolutionWidth + 60), sum(ResolutionWidth + 61), sum(ResolutionWidth + 62), sum(ResolutionWidth + 63), sum(ResolutionWidth + 64), sum(ResolutionWidth + 65), sum(ResolutionWidth + 66), sum(ResolutionWidth + 67), sum(ResolutionWidth + 68), sum(ResolutionWidth + 69), sum(ResolutionWidth + 70), sum(ResolutionWidth + 71), sum(ResolutionWidth + 72), sum(ResolutionWidth + 73), sum(ResolutionWidth + 74), sum(ResolutionWidth + 75), sum(ResolutionWidth + 76), sum(ResolutionWidth + 77), sum(ResolutionWidth + 78), sum(ResolutionWidth + 79), sum(ResolutionWidth + 80), sum(ResolutionWidth + 81), sum(ResolutionWidth + 82), sum(ResolutionWidth + 83), sum(ResolutionWidth + 84), sum(ResolutionWidth + 85), sum(ResolutionWidth + 86), sum(ResolutionWidth + 87), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM test.hits_s3;
SELECT SearchEngineID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM test.hits_s3 WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
SELECT WatchID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM test.hits_s3 WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY c, WatchID DESC LIMIT 10;
SELECT WatchID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM test.hits_s3 GROUP BY WatchID, ClientIP ORDER BY c, WatchID DESC LIMIT 10;
SELECT URL, count() AS c FROM test.hits_s3 GROUP BY URL ORDER BY c DESC LIMIT 10;
SELECT 1, URL, count() AS c FROM test.hits_s3 GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
SELECT ClientIP AS x, x - 1, x - 2, x - 3, count() AS c FROM test.hits_s3 GROUP BY x, x - 1, x - 2, x - 3 ORDER BY c DESC LIMIT 10;
SELECT URL, count() AS PageViews FROM test.hits_s3 WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND NOT DontCountHits AND NOT Refresh AND notEmpty(URL) GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
SELECT Title, count() AS PageViews FROM test.hits_s3 WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND NOT DontCountHits AND NOT Refresh AND notEmpty(Title) GROUP BY Title ORDER BY PageViews, Title DESC LIMIT 10;
SELECT URL, count() AS PageViews FROM test.hits_s3 WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND NOT Refresh AND IsLink AND NOT IsDownload GROUP BY URL ORDER BY PageViews DESC LIMIT 1000;
SELECT TraficSourceID, SearchEngineID, AdvEngineID, ((SearchEngineID = 0 AND AdvEngineID = 0) ? Referer : '') AS Src, URL AS Dst, count() AS PageViews FROM test.hits_s3 WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND NOT Refresh GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews, TraficSourceID DESC LIMIT 1000;
SELECT URLHash, EventDate, count() AS PageViews FROM test.hits_s3 WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND NOT Refresh AND TraficSourceID IN (-1, 6) AND RefererHash = halfMD5('http://example.ru/') GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 100;
SELECT WindowClientWidth, WindowClientHeight, count() AS PageViews FROM test.hits_s3 WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND NOT Refresh AND NOT DontCountHits AND URLHash = halfMD5('http://example.ru/') GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10000;
SELECT toStartOfMinute(EventTime) AS Minute, count() AS PageViews FROM test.hits_s3 WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-02' AND NOT Refresh AND NOT DontCountHits GROUP BY Minute ORDER BY Minute;

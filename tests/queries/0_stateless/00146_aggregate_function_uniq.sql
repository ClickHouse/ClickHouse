-- Tags: stateful
SELECT RegionID, uniqHLL12(WatchID) AS X FROM remote('127.0.0.{1,2}', test, hits) GROUP BY RegionID HAVING X > 100000 ORDER BY RegionID ASC;
SELECT RegionID, uniqCombined(WatchID) AS X FROM remote('127.0.0.{1,2}', test, hits) GROUP BY RegionID HAVING X > 100000 ORDER BY RegionID ASC;
SELECT abs(uniq(WatchID) - uniqExact(WatchID)) FROM test.hits;

SELECT RegionID, uniq(UserID) AS u FROM test.hits WHERE CounterID = 34 GROUP BY RegionID ORDER BY u DESC, RegionID LIMIT 10

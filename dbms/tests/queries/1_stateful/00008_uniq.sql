SELECT uniq(UserID), uniqIf(UserID, CounterID = 1143050), uniqIf(FUniqID, RegionID = 213) FROM test.hits

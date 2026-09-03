-- Tags: stateful, distributed


SELECT uniq(UserID), uniqIf(UserID, CounterID = 800784), uniqIf(FUniqID, RegionID = 213) FROM remote('127.0.0.{1,2}', test, hits)

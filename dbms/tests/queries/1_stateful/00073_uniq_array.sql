SELECT EventDate, uniqExact(UserID), length(groupUniqArray(UserID)), arrayUniq(groupArray(UserID)) FROM test.hits WHERE CounterID = 731962 GROUP BY EventDate ORDER BY EventDate;

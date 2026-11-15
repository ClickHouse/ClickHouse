-- Tags: stateful
SELECT CounterID, min(WatchID), max(WatchID) FROM test.hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20;
SELECT CounterID, min(WatchID), max(WatchID) FROM test.hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20 SETTINGS optimize_aggregation_in_order = 1

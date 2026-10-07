-- Columns of aggregate states produced by `+`, `*` and `CAST` must keep the state version of their type
-- (`AggregateFunction(1, uniq, ...)`), so that a later round trip through an arena (`groupArray`)
-- or through the wire (`remote`) reads the states back in the layout they were written in.

SELECT 'plus', toTypeName(s), hex(s) FROM (SELECT uniqState(number) + uniqState(number + 5) AS s FROM numbers(10));
SELECT 'plus via groupArray', hex(arrayJoin(groupArray(s))) FROM (SELECT uniqState(number) + uniqState(number + 5) AS s FROM numbers(10));
SELECT 'plus via remote', finalizeAggregation(s) FROM remote('127.0.0.{1,2}', view(SELECT uniqState(number) + uniqState(number + 5) AS s FROM numbers(10))) ORDER BY hex(s);

SELECT 'multiply', toTypeName(s), hex(s) FROM (SELECT uniqState(number) * 2 AS s FROM numbers(10));
SELECT 'multiply via groupArray', hex(arrayJoin(groupArray(s))) FROM (SELECT uniqState(number) * 3 AS s FROM numbers(10));
SELECT 'multiply via remote', finalizeAggregation(s) FROM remote('127.0.0.{1,2}', view(SELECT uniqState(number) * 2 AS s FROM numbers(10))) ORDER BY hex(s);

-- CAST keeps the version of the target type: the same states, spelled out in version 1 or in the legacy version 0.
SELECT 'cast v1', toTypeName(s), hex(s) FROM (SELECT CAST(uniqState(number) AS AggregateFunction(1, uniq, UInt64)) AS s FROM numbers(10));
SELECT 'cast v1 via groupArray', hex(arrayJoin(groupArray(s))) FROM (SELECT CAST(uniqState(number) AS AggregateFunction(1, uniq, UInt64)) AS s FROM numbers(10));
SELECT 'cast v0', toTypeName(s), hex(s) FROM (SELECT CAST(uniqState(number) AS AggregateFunction(uniq, UInt64)) AS s FROM numbers(10));
SELECT 'cast v0 via groupArray', hex(arrayJoin(groupArray(s))) FROM (SELECT CAST(uniqState(number) AS AggregateFunction(uniq, UInt64)) AS s FROM numbers(10));
SELECT 'cast v0 via remote', finalizeAggregation(s) FROM remote('127.0.0.{1,2}', view(SELECT CAST(uniqState(number) AS AggregateFunction(uniq, UInt64)) AS s FROM numbers(10))) ORDER BY hex(s);

-- `quantileDeterministic` is the other function whose state type spells out a version.
SELECT 'quantileDeterministic plus via groupArray', toTypeName(s), finalizeAggregation(arrayJoin(groupArray(s))) FROM (SELECT quantileDeterministicState(number, number) + quantileDeterministicState(number + 100, number) AS s FROM numbers(10)) GROUP BY toTypeName(s);
SELECT 'quantileDeterministic multiply via groupArray', finalizeAggregation(arrayJoin(groupArray(s))) FROM (SELECT quantileDeterministicState(number, number) * 2 AS s FROM numbers(10));
SELECT 'quantileDeterministic cast v0 via groupArray', toTypeName(s), finalizeAggregation(arrayJoin(groupArray(s))) FROM (SELECT CAST(quantileDeterministicState(number, number) AS AggregateFunction(quantileDeterministic, UInt64, UInt64)) AS s FROM numbers(10)) GROUP BY toTypeName(s);

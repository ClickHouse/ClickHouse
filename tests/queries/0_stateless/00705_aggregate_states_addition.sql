SET send_logs_level = 'fatal';
DROP TABLE IF EXISTS add_aggregate;
CREATE TABLE add_aggregate(a UInt32, b UInt32) ENGINE = Memory;

INSERT INTO add_aggregate VALUES(1, 2);
INSERT INTO add_aggregate VALUES(3, 1);

SELECT countMerge(x + y) FROM (SELECT countState(a) as x, countState(b) as y from add_aggregate);
SELECT sumMerge(x + y), sumMerge(x), sumMerge(y) FROM (SELECT sumState(a) as x, sumState(b) as y from add_aggregate);
SELECT sumMerge(x) FROM (SELECT sumState(a) + countState(b) as x FROM add_aggregate); -- { serverError 421 }
SELECT sumMerge(x) FROM (SELECT sumState(a) + sumState(toInt32(b)) as x FROM add_aggregate); -- { serverError 421 }

SELECT minMerge(x) FROM (SELECT minState(a) + minState(b) as x FROM add_aggregate);

SELECT uniqMerge(x + y) FROM (SELECT uniqState(a) as x, uniqState(b) as y FROM add_aggregate);

SELECT arraySort(groupArrayMerge(x + y)) FROM (SELECT groupArrayState(a) AS x, groupArrayState(b) as y FROM add_aggregate);
SELECT arraySort(groupUniqArrayMerge(x + y)) FROM (SELECT groupUniqArrayState(a) AS x, groupUniqArrayState(b) as y FROM add_aggregate);

SELECT uniqMerge(x + y) FROM (SELECT uniqState(65536, a) AS x, uniqState(b) AS y FROM add_aggregate); -- { serverError 421 }

DROP TABLE IF EXISTS add_aggregate;

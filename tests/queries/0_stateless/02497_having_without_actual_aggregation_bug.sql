select number from numbers_mt(10) having number >= 9;

select count() from numbers_mt(100) having count() > 1;

select queryID() as t from numbers(10) with totals having t = initialQueryID(); -- { serverError 48 }

-- this query works despite 'with total' doesn't make any sense due to this logic:
-- https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/InterpreterSelectQuery.cpp#L608-L610
select count() from (select queryID() as t from remote('127.0.0.{1..3}', numbers(10)) with totals having t = initialQueryID()) settings prefer_localhost_replica = 1;

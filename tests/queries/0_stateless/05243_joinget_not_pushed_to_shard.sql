-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/116838
-- A predicate pushed to remote shards as an AST must not carry `joinGet`. A `Join` table is local to
-- the server that holds it and is kept in sync with nothing, so the shard answers the call against
-- its own copy and filters by something the initiator never asked for - or throws `UNKNOWN_TABLE`
-- when it has no such table at all. Rows the shard drops never come back, because the initiator's
-- own filter only ever sees what the shard returned.
--
-- What decides this is `IFunctionBase::isDeterministic`. `JoinGetOverloadResolver` answers it
-- correctly, but the *resolved* function is what the conversion asks, and that one inherited the
-- default `true`.

-- The predicate travels as an AST only when the remote query is sent as text, and only the
-- `Distributed` path is under test here.
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_joinget_push;
DROP TABLE IF EXISTS j_joinget_push;

CREATE TABLE t_joinget_push (a UInt32, k UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_joinget_push SELECT number, number % 100 FROM numbers(1000);

CREATE TABLE j_joinget_push (k UInt32, v UInt32) ENGINE = Join(ANY, LEFT, k);
INSERT INTO j_joinget_push SELECT number, number * 10 FROM numbers(100);

SELECT 'the answer does not change';
SELECT count() FROM t_joinget_push
WHERE joinGet(currentDatabase() || '.j_joinget_push', 'v', k) >= 500;
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_joinget_push))
WHERE joinGet(currentDatabase() || '.j_joinget_push', 'v', k) >= 500;

SELECT 'and does not change for a conjunction either';
SELECT count() FROM t_joinget_push
WHERE joinGet(currentDatabase() || '.j_joinget_push', 'v', k) >= 500 AND a >= 500;
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_joinget_push))
WHERE joinGet(currentDatabase() || '.j_joinget_push', 'v', k) >= 500 AND a >= 500;

-- The shard must never be asked for `joinGet`. A conjunction still travels without it, so this says
-- the call is dropped rather than the whole push-down being turned off.
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_joinget_push))
WHERE joinGet(currentDatabase() || '.j_joinget_push', 'v', k) >= 500
SETTINGS log_comment = '05243_joinget_only' FORMAT Null;
SELECT count() FROM (SELECT * FROM remote('127.0.0.2', currentDatabase(), t_joinget_push))
WHERE joinGet(currentDatabase() || '.j_joinget_push', 'v', k) >= 500 AND a >= 500
SETTINGS log_comment = '05243_joinget_and' FORMAT Null;
SYSTEM FLUSH LOGS query_log;

SELECT 'shipped to the shard: log_comment, carries joinGet, carries a filter';
SELECT log_comment, countIf(query LIKE '%joinGet%') > 0, countIf(query LIKE '%HAVING%') > 0
FROM system.query_log
-- The secondary queries do not carry `current_database`, so match them by the database they read.
WHERE has(databases, currentDatabase())
    AND event_date >= yesterday() AND event_time > now() - 600 AND type = 'QueryFinish' AND is_initial_query = 0
    AND log_comment IN ('05243_joinget_only', '05243_joinget_and') AND query LIKE '%t_joinget_push%'
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_joinget_push;
DROP TABLE j_joinget_push;

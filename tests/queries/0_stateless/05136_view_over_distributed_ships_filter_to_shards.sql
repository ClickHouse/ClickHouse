-- Tags: distributed

-- A query over a view whose body reads a `Distributed` table has to ship the outer `WHERE`
-- to the shards. Otherwise every shard scans its whole local table and sends all of the rows
-- back to the initiator, which then does the filtering.
-- https://github.com/ClickHouse/ClickHouse/issues/78188

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
-- Take the network path, so that the shard really receives a query and logs it.
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 05136_local;
DROP TABLE IF EXISTS 05136_distributed;
DROP VIEW IF EXISTS 05136_view;

CREATE TABLE 05136_local (id UInt64, value UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO 05136_local SELECT number, number * 2 FROM numbers(1000000) SETTINGS max_insert_threads = 1;

CREATE TABLE 05136_distributed AS 05136_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 05136_local);

CREATE VIEW 05136_view AS SELECT * FROM 05136_distributed;

SELECT * FROM 05136_view WHERE id = 2 SETTINGS log_comment = '05136_view_over_distributed';

SYSTEM FLUSH LOGS query_log;

-- The shard reads a couple of granules instead of the whole table, and sends back
-- the single matching row instead of a million rows.
SELECT read_rows < 100000, result_rows
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND log_comment = '05136_view_over_distributed');

DROP VIEW 05136_view;
DROP TABLE 05136_distributed;
DROP TABLE 05136_local;

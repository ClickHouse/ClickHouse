-- Tags: zookeeper
-- Tag zookeeper: the inner tables are argument-less replicated tables using the default replica path.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/clickhouse/05060_time_series_truncate_in_replicated_database/{database}', 'shard1', 'replica1') FORMAT Null;
USE {CLICKHOUSE_DATABASE_1:Identifier};

SET allow_experimental_time_series_table = 1;
SET default_table_engine = 'ReplicatedMergeTree';

CREATE TABLE ts ENGINE = TimeSeries FORMAT Null;

-- Both engine families count: Cloud runs rewrite the setting above to `SharedMergeTree`.
SELECT 'replicated_inner_tables', count() FROM system.tables
WHERE database = currentDatabase() AND (engine LIKE 'Replicated%MergeTree' OR engine LIKE 'Shared%MergeTree');

INSERT INTO ts(metric_name, tags, time_series) VALUES ('m', map('a', 'b'), [(now64(3), 1)]);
SELECT 'samples_before', count() FROM ts;

TRUNCATE TABLE ts FORMAT Null;
SELECT 'samples_after', count() FROM ts;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;

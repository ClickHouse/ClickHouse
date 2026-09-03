-- Tags: zookeeper, no-replicated-database, no-ordinary-database
CREATE TABLE hourly_data
    (domain_name String, event_time DateTime, count_views UInt64)
    ENGINE = MergeTree ORDER BY (domain_name, event_time);

CREATE TABLE monthly_aggregated_data
    (domain_name String, month Date, sumCountViews AggregateFunction(sum, UInt64))
    ENGINE = AggregatingMergeTree ORDER BY (domain_name, month);


CREATE MATERIALIZED VIEW monthly_aggregated_data_mv
    TO monthly_aggregated_data
    AS
    SELECT
        toDate(toStartOfMonth(event_time)) AS month,
        domain_name,
        sumState(count_views) AS sumCountViews
    FROM hourly_data
    GROUP BY
        domain_name,
        month;

INSERT INTO hourly_data (domain_name, event_time, count_views)
    VALUES ('clickhouse.com', '2019-01-01 10:00:00', 1), ('clickhouse.com', '2019-02-02 00:00:00', 2), ('clickhouse.com', '2019-02-01 00:00:00', 3);

SELECT sumMerge(sumCountViews) as sumCountViews FROM monthly_aggregated_data_mv;
SELECT count() FROM hourly_data;

DETACH TABLE hourly_data;
DETACH TABLE monthly_aggregated_data;
ATTACH TABLE hourly_data AS REPLICATED;
ATTACH TABLE monthly_aggregated_data AS REPLICATED;
SYSTEM RESTORE REPLICA hourly_data;
SYSTEM RESTORE REPLICA monthly_aggregated_data;

SELECT name, engine FROM system.tables WHERE database=currentDatabase();

INSERT INTO hourly_data (domain_name, event_time, count_views)
    VALUES ('clickhouse.com', '2019-01-01 10:00:00', 1), ('clickhouse.com', '2019-02-02 00:00:00', 2), ('clickhouse.com', '2019-02-01 00:00:00', 3);

SELECT sumMerge(sumCountViews) as sumCountViews FROM monthly_aggregated_data_mv;
SELECT count() FROM hourly_data;

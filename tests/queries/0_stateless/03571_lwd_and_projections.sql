DROP TABLE IF EXISTS weird_projections;

CREATE TABLE weird_projections(
    `account_id` UInt64,
    `user_id` String,
    PROJECTION events_by_day_proj
    (
        SELECT
            account_id,
            countDistinct(user_id) AS total_users
        GROUP BY
            account_id
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/test', '1')
ORDER BY (account_id)
SETTINGS index_granularity = 8192, lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO weird_projections SELECT 134 as account_id, toString(account_id) as user_id FROM numbers(10000);
INSERT INTO weird_projections SELECT 132 as account_id, toString(account_id) as user_id FROM numbers(10000);

OPTIMIZE TABLE weird_projections FINAL;

DELETE FROM weird_projections WHERE account_id = 134;

DROP TABLE IF EXISTS weird_projections;

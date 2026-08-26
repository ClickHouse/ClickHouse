-- Tags: long, no-random-settings, no-random-merge-tree-settings

-- Test that read-in-order optimization works through CreatingSetsStep (IN subqueries)
-- when used with full_sorting_merge join.
-- Previously, DelayedCreatingSetsStep between SortingStep and ReadFromMergeTree
-- prevented findReadingStep from reaching the reading step, causing a fallback
-- to ReadPool (Thread) instead of ReadPoolInOrder (InOrder).
--
-- The no-random-settings / no-random-merge-tree-settings tags are required because
-- this test asserts on the EXPLAIN PIPELINE shape, which is sensitive to many
-- unrelated randomized knobs (merge-tree layout, range splitting, two-level merge
-- threshold, etc.).

DROP TABLE IF EXISTS data_a;
DROP TABLE IF EXISTS data_b;
DROP TABLE IF EXISTS reports_a;
DROP TABLE IF EXISTS reports_b;

CREATE TABLE data_a (tenant LowCardinality(String), report_id UUID, date Date, key String, value_a Int32) ENGINE = MergeTree() ORDER BY (tenant, date, key);
CREATE TABLE data_b (tenant LowCardinality(String), report_id UUID, date Date, key String, value_b Int32) ENGINE = MergeTree() ORDER BY (tenant, date, key);

CREATE TABLE reports_a (tenant LowCardinality(String), date Date, account_id String, reported_at DateTime64(3), report_id UUID) ENGINE = ReplacingMergeTree(reported_at) ORDER BY (tenant, date, account_id);
CREATE TABLE reports_b (tenant LowCardinality(String), date Date, account_id String, reported_at DateTime64(3), report_id UUID) ENGINE = ReplacingMergeTree(reported_at) ORDER BY (tenant, date, account_id);

-- reports tables: 3 inserts each to create 3 parts
INSERT INTO reports_a SELECT 'x', toDate('2024-01-01') + number % 365, toString(number % 10000), now64(3), generateUUIDv4() FROM numbers(100000);
INSERT INTO reports_a SELECT 'x', toDate('2024-01-01') + number % 365, toString(100000 + number % 10000), now64(3), generateUUIDv4() FROM numbers(100000);
INSERT INTO reports_a SELECT 'x', toDate('2024-01-01') + number % 365, toString(200000 + number % 10000), now64(3), generateUUIDv4() FROM numbers(100000);

INSERT INTO reports_b SELECT 'x', toDate('2024-01-01') + number % 365, toString(number % 10000), now64(3), generateUUIDv4() FROM numbers(100000);
INSERT INTO reports_b SELECT 'x', toDate('2024-01-01') + number % 365, toString(100000 + number % 10000), now64(3), generateUUIDv4() FROM numbers(100000);
INSERT INTO reports_b SELECT 'x', toDate('2024-01-01') + number % 365, toString(200000 + number % 10000), now64(3), generateUUIDv4() FROM numbers(100000);

-- data tables: 5 inserts each to create 5 parts
INSERT INTO data_a SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(number % 1000), number FROM numbers(1000000);
INSERT INTO data_a SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(1000 + number % 1000), number FROM numbers(1000000);
INSERT INTO data_a SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(2000 + number % 1000), number FROM numbers(1000000);
INSERT INTO data_a SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(3000 + number % 1000), number FROM numbers(1000000);
INSERT INTO data_a SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(4000 + number % 1000), number FROM numbers(1000000);

INSERT INTO data_b SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(number % 1000), number FROM numbers(1000000);
INSERT INTO data_b SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(1000 + number % 1000), number FROM numbers(1000000);
INSERT INTO data_b SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(2000 + number % 1000), number FROM numbers(1000000);
INSERT INTO data_b SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(3000 + number % 1000), number FROM numbers(1000000);
INSERT INTO data_b SELECT 'x', generateUUIDv4(), toDate('2024-01-01') + number % 365, toString(4000 + number % 1000), number FROM numbers(1000000);

SET max_threads = 4, query_plan_read_in_order = 1, optimize_read_in_order = 1, query_plan_read_in_order_through_join = 1;

-- full_sorting_merge JOIN with IN subqueries: data tables must NOT use ReadPool/Thread
SELECT
    countIf(explain LIKE '%ReadPool, algorithm: Thread%') AS has_thread
FROM (
    EXPLAIN PIPELINE
    WITH
        a AS (
            SELECT tenant, date, key, value_a
            FROM data_a
            WHERE tenant = 'x'
                AND date BETWEEN '2024-01-01' AND '2024-12-31'
                AND report_id IN (
                    SELECT report_id FROM reports_a FINAL
                    WHERE tenant = 'x'
                        AND date BETWEEN '2024-01-01' AND '2024-12-31'
                )
        ),
        b AS (
            SELECT tenant, date, key, value_b
            FROM data_b
            WHERE tenant = 'x'
                AND date BETWEEN '2024-01-01' AND '2024-12-31'
                AND report_id IN (
                    SELECT report_id FROM reports_b FINAL
                    WHERE tenant = 'x'
                        AND date BETWEEN '2024-01-01' AND '2024-12-31'
                )
        )
    SELECT date, sum(a.value_a), sum(b.value_b)
    FROM a
    FULL OUTER JOIN b USING (tenant, date, key)
    GROUP BY date
    SETTINGS join_algorithm = 'full_sorting_merge'
);

DROP TABLE data_a;
DROP TABLE data_b;
DROP TABLE reports_a;
DROP TABLE reports_b;

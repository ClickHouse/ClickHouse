DROP TABLE IF EXISTS base_table;
DROP TABLE IF EXISTS target_table;
DROP TABLE IF EXISTS mv_from_base_to_target;
DROP TABLE IF EXISTS mv_with_storage;
DROP TABLE IF EXISTS other_table_1;
DROP TABLE IF EXISTS other_table_2;

CREATE TABLE base_table (date DateTime, id String, cost Float64) ENGINE = MergeTree() ORDER BY date;
CREATE TABLE target_table (id String, total AggregateFunction(sum, Float64)) ENGINE = MergeTree() ORDER BY id;
CREATE MATERIALIZED VIEW mv_from_base_to_target TO target_table AS Select id, sumState(cost) AS total FROM base_table GROUP BY id;
CREATE MATERIALIZED VIEW mv_with_storage ENGINE=MergeTree() ORDER BY id AS Select id, sumState(cost) FROM base_table GROUP BY id;

CREATE TABLE other_table_1 AS mv_with_storage;
CREATE TABLE other_table_2 AS mv_from_base_to_target; -- { serverError INCORRECT_QUERY }

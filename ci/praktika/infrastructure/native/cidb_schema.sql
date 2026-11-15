-- Praktika CI DB schema. Re-applied on every boot of cidb instances by the
-- bootstrap systemd unit (see user_data_cidb.sh). All statements must be
-- idempotent (CREATE ... IF NOT EXISTS / ON CLUSTER if added later).

CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.checks
(
    `pull_request_number` UInt32,
    `commit_sha` LowCardinality(String),
    `check_name` LowCardinality(String),
    `check_status` LowCardinality(String),
    `check_duration_ms` UInt64,
    `check_start_time` DateTime,
    `test_name` LowCardinality(String),
    `test_status` LowCardinality(String),
    `test_duration_ms` UInt64,
    `report_url` String,
    `pull_request_url` String,
    `commit_url` String,
    `task_url` String,
    `base_ref` String,
    `base_repo` String,
    `head_ref` String,
    `head_repo` String,
    `test_context_raw` String,
    `instance_type` LowCardinality(String),
    `instance_id` String,
    `workflow_name` LowCardinality(String),
    `extensions` JSON
)
-- Keeper path uses {shard}/{database}/{table} (not {uuid}) because the
-- {uuid} macro requires ON CLUSTER or the Replicated database engine in
-- recent ClickHouse versions; see UNKNOWN_TABLE/BAD_ARGUMENTS error from
-- 26.x. {database}/{table} are auto-set per-table and stable across recreates.
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
ORDER BY check_start_time
SETTINGS index_granularity = 8192;

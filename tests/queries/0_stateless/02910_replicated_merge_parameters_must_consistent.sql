-- Tags: zookeeper, no-replicated-database
CREATE TABLE t
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64,
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t/', 'r1', legacy_ver)
ORDER BY id;

CREATE TABLE t_r
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t/', 'r2')
ORDER BY id; -- { serverError METADATA_MISMATCH }

CREATE TABLE t3
(
    `key` UInt64,
    `metrics1` UInt64,
    `metrics2` UInt64
)
ENGINE = ReplicatedSummingMergeTree('/tables/{database}/t3/', 'r1', metrics1)
ORDER BY key;

CREATE TABLE t3_r
(
    `key` UInt64,
    `metrics1` UInt64,
    `metrics2` UInt64
)
ENGINE = ReplicatedSummingMergeTree('/tables/{database}/t3/', 'r2', metrics2)
ORDER BY key; -- { serverError METADATA_MISMATCH }

CREATE TABLE t4
(
    `key` UInt32,
    `Path` String,
    `Time` DateTime('UTC'),
    `Value` Float64,
    `Version` UInt32,
    `col` UInt64
)
ENGINE = ReplicatedGraphiteMergeTree('/tables/{database}/t4/', 'r1', 'graphite_rollup')
ORDER BY key;

CREATE TABLE t4_r
(
    `key` UInt32,
    `Path` String,
    `Time` DateTime('UTC'),
    `Value` Float64,
    `Version` UInt32,
    `col` UInt64
)
ENGINE = ReplicatedGraphiteMergeTree('/tables/{database}/t4/', 'r2', 'graphite_rollup_alternative')
ORDER BY key; -- { serverError METADATA_MISMATCH }

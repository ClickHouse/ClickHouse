-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CREATE TABLE t
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64,
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t/', 'r1', legacy_ver)
ORDER BY id;

CREATE TABLE t_r_ok
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64,
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t/', 'r2', legacy_ver)
ORDER BY id;

CREATE TABLE t_r_error
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t/', 'r3')
ORDER BY id; -- { serverError METADATA_MISMATCH }

CREATE TABLE t2
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64,
    `deleted` UInt8
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t2/', 'r1', legacy_ver)
ORDER BY id;

CREATE TABLE t2_r_ok
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64,
    `deleted` UInt8
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t2/', 'r2', legacy_ver)
ORDER BY id;

CREATE TABLE t2_r_error
(
    `id` UInt64,
    `val` String,
    `legacy_ver` UInt64,
    `deleted` UInt8
)
ENGINE = ReplicatedReplacingMergeTree('/tables/{database}/t2/', 'r3', legacy_ver, deleted)
ORDER BY id; -- { serverError METADATA_MISMATCH }

CREATE TABLE t3
(
    `key` UInt64,
    `metrics1` UInt64,
    `metrics2` UInt64
)
ENGINE = ReplicatedSummingMergeTree('/tables/{database}/t3/', 'r1', metrics1)
ORDER BY key;

CREATE TABLE t3_r_ok
(
    `key` UInt64,
    `metrics1` UInt64,
    `metrics2` UInt64
)
ENGINE = ReplicatedSummingMergeTree('/tables/{database}/t3/', 'r2', metrics1)
ORDER BY key;


CREATE TABLE t3_r_error
(
    `key` UInt64,
    `metrics1` UInt64,
    `metrics2` UInt64
)
ENGINE = ReplicatedSummingMergeTree('/tables/{database}/t3/', 'r3', metrics2)
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

CREATE TABLE t4_r_ok
(
    `key` UInt32,
    `Path` String,
    `Time` DateTime('UTC'),
    `Value` Float64,
    `Version` UInt32,
    `col` UInt64
)
ENGINE = ReplicatedGraphiteMergeTree('/tables/{database}/t4/', 'r2', 'graphite_rollup')
ORDER BY key;

CREATE TABLE t4_r_error
(
    `key` UInt32,
    `Path` String,
    `Time` DateTime('UTC'),
    `Value` Float64,
    `Version` UInt32,
    `col` UInt64
)
ENGINE = ReplicatedGraphiteMergeTree('/tables/{database}/t4/', 'r3', 'graphite_rollup_alternative')
ORDER BY key; -- { serverError METADATA_MISMATCH }

-- https://github.com/ClickHouse/ClickHouse/issues/58451
CREATE TABLE t4_r_error_2
(
    `key` UInt32,
    `Path` String,
    `Time` DateTime('UTC'),
    `Value` Float64,
    `Version` UInt32,
    `col` UInt64
)
ENGINE = ReplicatedGraphiteMergeTree('/tables/{database}/t4/', 'r4', 'graphite_rollup_alternative_no_function')
ORDER BY key; -- { serverError METADATA_MISMATCH }
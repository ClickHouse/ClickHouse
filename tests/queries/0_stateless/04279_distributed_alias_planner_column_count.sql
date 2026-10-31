-- Tags: distributed

-- Two distinct scenarios that both fail at the planner name-first reconciliation:
-- the remote column count diverges from what the initiator expects.
-- Both are fixed by makeConvertingActionsPreferNameThenPosition + __aliasMarker injection.

SELECT '---- single_hop_same_expression ----';
-- Regression coverage for distributed ORDER BY + ALIAS columns with identical expressions.
-- Related issue: https://github.com/ClickHouse/ClickHouse/issues/79916

DROP TABLE IF EXISTS test_alias_same_expr_remote;

CREATE TABLE test_alias_same_expr_remote
(
    dt DateTime64(3),
    String_7 String,
    alias_String_7_0 String ALIAS String_7,
    alias_String_7_1 String ALIAS String_7
)
ENGINE = MergeTree()
ORDER BY dt;

INSERT INTO test_alias_same_expr_remote VALUES ('1999-03-29T01:15:33', '');

SELECT 'first';
SELECT dt, alias_String_7_0, alias_String_7_1
FROM remote('127.0.0.{1,2}', currentDatabase(), test_alias_same_expr_remote)
LIMIT 1
SETTINGS enable_alias_marker = 1;

SELECT 'second';
SELECT dt, alias_String_7_0, alias_String_7_1
FROM remote('127.0.0.{1,2}', currentDatabase(), test_alias_same_expr_remote)
ORDER BY dt
LIMIT 1
SETTINGS enable_analyzer = 0, enable_alias_marker = 1;

SELECT 'third';
SELECT dt, alias_String_7_0, alias_String_7_1
FROM remote('127.0.0.{1,2}', currentDatabase(), test_alias_same_expr_remote)
ORDER BY dt
LIMIT 1
SETTINGS enable_analyzer = 1, enable_alias_marker = 1;

SELECT 'fifth';
SELECT dt, alias_String_7_0, alias_String_7_1
FROM remote('127.0.0.{1,2}', currentDatabase(), test_alias_same_expr_remote)
ORDER BY dt
LIMIT 1
SETTINGS enable_analyzer = 1, serialize_query_plan = 1, enable_alias_marker = 1;

SELECT 'sixth';
SELECT alias_String_7_0 AS query_alias_0, alias_String_7_1 AS query_alias_1
FROM remote('127.0.0.{1,2}', currentDatabase(), test_alias_same_expr_remote)
ORDER BY dt
LIMIT 1
SETTINGS enable_analyzer = 1, enable_alias_marker = 1
FORMAT TSVWithNames;

SELECT 'seventh';
SELECT alias_String_7_0, alias_String_7_1
FROM remote('127.0.0.{1,2}', currentDatabase(), test_alias_same_expr_remote)
ORDER BY dt
LIMIT 1
SETTINGS enable_analyzer = 1, enable_alias_marker = 1
FORMAT TSVWithNames;

DROP TABLE test_alias_same_expr_remote;

SELECT '---- multi_hop_double_aliases ----';
DROP TABLE IF EXISTS test_dod_double_alias_outer;
DROP TABLE IF EXISTS test_dod_double_alias_inner;
DROP TABLE IF EXISTS test_dod_double_alias_local;

CREATE TABLE test_dod_double_alias_local
(
    x UInt64
)
ENGINE = MergeTree()
ORDER BY x;

INSERT INTO test_dod_double_alias_local VALUES (1), (2), (10);

CREATE TABLE test_dod_double_alias_inner
(
    x UInt64,
    a UInt64 ALIAS 2,
    b UInt64 ALIAS 2,
    inner_c UInt64 ALIAS x + 1,
    inner_d UInt64 ALIAS x + 1
)
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), test_dod_double_alias_local);

CREATE TABLE test_dod_double_alias_outer
(
    x UInt64,
    inner_c UInt64,
    a UInt64 ALIAS 1,
    b UInt64 ALIAS 1,
    c UInt64 ALIAS inner_c,
    d UInt64 ALIAS inner_c,
    inner_d UInt64
)
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), test_dod_double_alias_inner);

SELECT 'prefer_localhost_replica_0';
SELECT x, a, b, c, d, inner_c, inner_d
FROM test_dod_double_alias_outer
ORDER BY x
SETTINGS
    enable_analyzer = 1,
    enable_alias_marker = 1,
    prefer_localhost_replica = 0,
    enable_parallel_replicas = 0,
    max_parallel_replicas = 1,
    parallel_replicas_local_plan = 0
FORMAT TSVWithNames;

SELECT 'prefer_localhost_replica_1';
SELECT x, a, b, c, d, inner_c, inner_d
FROM test_dod_double_alias_outer
ORDER BY x
SETTINGS
    enable_analyzer = 1,
    enable_alias_marker = 1,
    prefer_localhost_replica = 1,
    enable_parallel_replicas = 0,
    max_parallel_replicas = 1,
    parallel_replicas_local_plan = 0
FORMAT TSVWithNames;

SELECT 'prefer_localhost_replica_0_serialize_query_plan_1';
SELECT x, a, b, c, d, inner_c, inner_d
FROM test_dod_double_alias_outer
ORDER BY x
SETTINGS
    enable_analyzer = 1,
    enable_alias_marker = 1,
    prefer_localhost_replica = 0,
    enable_parallel_replicas = 0,
    max_parallel_replicas = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1
FORMAT TSVWithNames;

SELECT 'prefer_localhost_replica_1_serialize_query_plan_1';
SELECT x, a, b, c, d, inner_c, inner_d
FROM test_dod_double_alias_outer
ORDER BY x
SETTINGS
    enable_analyzer = 1,
    enable_alias_marker = 1,
    prefer_localhost_replica = 1,
    enable_parallel_replicas = 0,
    max_parallel_replicas = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1
FORMAT TSVWithNames;

DROP TABLE test_dod_double_alias_outer;
DROP TABLE test_dod_double_alias_inner;
DROP TABLE test_dod_double_alias_local;

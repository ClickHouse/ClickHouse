-- Tags: zookeeper

-- Query parameter substitution rewrites `{param:Type}` into `_CAST(value, 'Type')`, and the
-- serialized mutation entry must be parseable back when it is re-read from ZooKeeper.

DROP TABLE IF EXISTS t_mutation_param_r SYNC;

CREATE TABLE t_mutation_param_r (id UInt64, d Date, flag Nullable(Bool))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_param_r', 'r1')
PARTITION BY toYYYYMMDD(d) ORDER BY id;

INSERT INTO t_mutation_param_r SELECT number, '2026-06-24', NULL FROM numbers(10);
INSERT INTO t_mutation_param_r SELECT number + 100, '2026-06-25', NULL FROM numbers(10);

SET param_day = '20260624';

ALTER TABLE t_mutation_param_r UPDATE flag = true IN PARTITION {day:UInt32} WHERE toYYYYMMDD(d) = {day:UInt32} SETTINGS mutations_sync = 2;

SELECT countIf(flag), count() FROM t_mutation_param_r;

-- Make sure the mutation entries written to ZooKeeper can be parsed back when the table is loaded.
DETACH TABLE t_mutation_param_r;
ATTACH TABLE t_mutation_param_r;

SELECT countIf(flag), count() FROM t_mutation_param_r;

SYSTEM RESTART REPLICA t_mutation_param_r;

SELECT countIf(flag), count() FROM t_mutation_param_r;

DROP TABLE t_mutation_param_r SYNC;

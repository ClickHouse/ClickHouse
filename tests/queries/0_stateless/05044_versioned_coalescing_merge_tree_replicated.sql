-- Tags: zookeeper

DROP TABLE IF EXISTS t_vcmt_replicated;

CREATE TABLE t_vcmt_replicated
(
    key UInt64,
    version UInt64,
    a Nullable(UInt64),
    b Nullable(String)
)
ENGINE = ReplicatedVersionedCoalescingMergeTree('/clickhouse/tables/{database}/05044_vcmt', 'r1', version)
ORDER BY key;

SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_vcmt_replicated';

INSERT INTO t_vcmt_replicated VALUES (1, 2, 42, NULL);
INSERT INTO t_vcmt_replicated VALUES (1, 1, 10, 'first');

SELECT * FROM t_vcmt_replicated FINAL;

OPTIMIZE TABLE t_vcmt_replicated FINAL;
SELECT * FROM t_vcmt_replicated;

DROP TABLE t_vcmt_replicated;

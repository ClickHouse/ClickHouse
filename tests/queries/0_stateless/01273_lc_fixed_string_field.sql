CREATE TABLE t
(
    `d` Date,
    `s` LowCardinality(FixedString(3)),
    `c` UInt32
)
ENGINE = SummingMergeTree()
PARTITION BY d
ORDER BY (d, s);

INSERT INTO t (d, s, c) VALUES ('2020-01-01', 'ABC', 1);
INSERT INTO t (d, s, c) VALUES ('2020-01-01', 'ABC', 2);

OPTIMIZE TABLE t;
SELECT * FROM t;

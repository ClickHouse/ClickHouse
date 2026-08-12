DROP TABLE IF EXISTS t SYNC;

CREATE TABLE t
(
    `product` Enum8('IU' = 1, 'WS' = 2),
    `machine` String,
    `branch` String,
    `generated_time` DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1')
PARTITION BY (product, toYYYYMM(generated_time))
ORDER BY (product, machine, branch, generated_time);

INSERT INTO t VALUES ('IU', 'Hello', 'World', '2000-01-01 01:02:03'), ('WS', 'Hello', 'World', '2000-02-03 04:05:06');

ALTER TABLE t
    MODIFY COLUMN `product` Enum8('IU' = 1, 'WS' = 2, 'PS' = 3)
SETTINGS alter_sync = 2;

SELECT product
FROM t
GROUP BY product
ORDER BY product;

DROP TABLE t SYNC;

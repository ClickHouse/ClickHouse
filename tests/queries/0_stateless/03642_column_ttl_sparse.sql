DROP TABLE IF EXISTS ttl_sparse_repro;

CREATE TABLE ttl_sparse_repro
(
    a UInt64,
    dt DateTime,
    b UInt64 TTL dt + INTERVAL 2 SECOND, c UInt64
)
ENGINE = MergeTree ORDER BY a SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO ttl_sparse_repro SELECT number, now(), 1, if (number % 23 = 0, 1, 0) FROM numbers(10000);

SELECT sum(c) FROM ttl_sparse_repro;

SELECT sleep(3) FORMAT Null;

OPTIMIZE TABLE ttl_sparse_repro FINAL;
SELECT sum(c) FROM ttl_sparse_repro;

OPTIMIZE TABLE ttl_sparse_repro FINAL;
SELECT sum(c) FROM ttl_sparse_repro;

OPTIMIZE TABLE ttl_sparse_repro FINAL;
SELECT sum(c) FROM ttl_sparse_repro;

DROP TABLE IF EXISTS ttl_sparse_repro;

-- { echo ON }

DROP TABLE IF EXISTS desc_pk;

CREATE TABLE desc_pk (`a` UInt32) ENGINE = MergeTree ORDER BY (a DESC) SETTINGS allow_experimental_reverse_key = 1;

INSERT INTO desc_pk SELECT * FROM numbers(10);

SELECT min(a), max(a) FROM desc_pk;

DROP TABLE desc_pk;

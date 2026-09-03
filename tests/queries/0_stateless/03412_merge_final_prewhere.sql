DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tmerge;

CREATE TABLE t
(
    k Int64,
    dt DateTime,
    s String
)
ENGINE = ReplacingMergeTree
ORDER BY (k, dt);

CREATE TABLE tmerge AS t ENGINE = Merge(currentDatabase(), '^t$');

INSERT INTO t
SELECT number, '2020-01-01 00:00:00', '' FROM numbers(10);

SELECT count() FROM tmerge FINAL PREWHERE dt >= '2020-01-01 00:00:00';

DROP TABLE tmerge;
DROP TABLE t;

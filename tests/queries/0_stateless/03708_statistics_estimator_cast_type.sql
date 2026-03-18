-- Tags: no-fasttest

CREATE TABLE dt64test
(
	    `dt64_column` DateTime64(3),
	    `dt_column` DateTime DEFAULT toDateTime(dt64_column)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt64_column)
ORDER BY dt64_column SETTINGS auto_statistics_types='tdigest';

SET allow_statistics_optimize = 1;

INSERT INTO dt64test (`dt64_column`) VALUES ('2020-01-13 13:37:00');

SELECT 'dt < const dt64' FROM dt64test WHERE dt_column < toDateTime64('2020-01-13 13:37:00', 3);

CREATE TABLE t1 (c0 Decimal(18,0)) ENGINE = MergeTree() ORDER BY (c0) SETTINGS auto_statistics_types='countmin';
INSERT INTO TABLE t1(c0) VALUES (1);

SELECT c0 = 6812671276462221925::Int64 FROM t1;
SELECT 1 FROM t1 WHERE c0 = 6812671276462221925::Int64;


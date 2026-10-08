-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/70058
-- Previously failed with:
--   Code: 10. NOT_FOUND_COLUMN_IN_BLOCK: Not found column res_sens_k in block:
--   while executing 'INPUT : 0 -> res_sens_k Array(LowCardinality(String)) : 0'.
-- The combination of arrayJoin(LowCardinality(...)) with a `set` skipping index
-- on another column used to break query analysis.

DROP TABLE IF EXISTS t_70058;

CREATE TABLE t_70058
(
    ts DateTime,
    method LowCardinality(String),
    res_sens_k Array(LowCardinality(String)),
    INDEX method_index method TYPE set(15) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 8192;

INSERT INTO t_70058 VALUES ('2999-01-01 00:00:00', 'GET',  ['key1', 'key2']);
INSERT INTO t_70058 VALUES ('2999-01-02 00:00:00', 'POST', ['key3', '']);
INSERT INTO t_70058 VALUES ('2999-01-03 00:00:00', 'HEAD', ['key4']);

SELECT count()
FROM t_70058
WHERE (ts > today())
  AND (method NOT IN ['HEAD', 'OPTIONS', 'CONNECT', 'WS'])
  AND (arrayJoin(res_sens_k) != '');

DROP TABLE t_70058;

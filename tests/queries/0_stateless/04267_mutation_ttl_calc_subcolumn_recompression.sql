-- Tags: no-replicated-database

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_calc_subcol;
CREATE TABLE t_ttl_calc_subcol
(
    k UInt64,
    d1 DateTime('UTC'),
    t Tuple(d2 DateTime('UTC')),
    payload String
)
ENGINE = MergeTree
ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)),
    t.d2 + INTERVAL 2 DAY RECOMPRESS CODEC(ZSTD(5))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_ttl_calc_subcol VALUES (1, '2090-01-01 00:00:00', tuple(toDateTime('2091-01-01 00:00:00', 'UTC')), 'keep');

ALTER TABLE t_ttl_calc_subcol UPDATE d1 = toDateTime('2092-01-01 00:00:00', 'UTC') WHERE 1;

SELECT k, d1, t.d2, payload FROM t_ttl_calc_subcol;

DROP TABLE t_ttl_calc_subcol;

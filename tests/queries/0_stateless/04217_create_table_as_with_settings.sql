-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/21389
-- `CREATE TABLE t2 AS t1 SETTINGS ...` (without explicit ENGINE) used to
-- silently treat SETTINGS as query-level and discard them from the new table.

DROP TABLE IF EXISTS t_21389_src;
DROP TABLE IF EXISTS t_21389_dst;

CREATE TABLE t_21389_src
(
    rank UInt32,
    domain String,
    log String CODEC(ZSTD(6)),
    content String CODEC(ZSTD(6))
)
ENGINE = MergeTree
ORDER BY domain;

CREATE TABLE t_21389_dst AS t_21389_src SETTINGS max_compress_block_size = 65536;

SHOW CREATE TABLE t_21389_dst FORMAT TSVRaw;

DROP TABLE t_21389_dst;
DROP TABLE t_21389_src;

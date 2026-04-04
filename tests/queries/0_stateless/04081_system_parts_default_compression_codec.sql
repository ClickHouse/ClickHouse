-- Tags: no-random-settings

-- Test that system.parts.default_compression_codec reports the actual codec
-- used for the part, not the server-wide default.
-- https://github.com/ClickHouse/ClickHouse/issues/84440

DROP TABLE IF EXISTS t_default_codec;

CREATE TABLE t_default_codec
(
    x String
)
ENGINE = MergeTree
ORDER BY x
SETTINGS default_compression_codec = 'ZSTD(3)';

INSERT INTO t_default_codec VALUES ('hello');

SELECT default_compression_codec
FROM system.parts
WHERE database = currentDatabase() AND table = 't_default_codec' AND active;

DROP TABLE t_default_codec;

-- https://github.com/ClickHouse/ClickHouse/issues/83832
CREATE TABLE t0 (c0 Int128) ENGINE = Memory;
INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int128', 9487793134203413997, 2902, 1) LIMIT 100;
SELECT count() FROM format(JSONCompactEachRow, 'c0 Int128', (SELECT arrayStringConcat(groupArray(formatRow('JSONCompactEachRow', c0)), '\n') FROM t0)) SETTINGS min_chunk_bytes_for_parallel_parsing = 4;

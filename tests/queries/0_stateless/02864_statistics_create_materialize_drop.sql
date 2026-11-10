-- Tags: no-fasttest

DROP TABLE IF EXISTS tab SYNC;

SET allow_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types=1;
SET mutations_sync = 2;


SELECT 'Test create statistics:';

CREATE TABLE tab
(
    a LowCardinality(Int64),
    b LowCardinality(Nullable(String)),
    c LowCardinality(Nullable(Int64)),
    d DateTime,
    pk String,
) Engine = MergeTree() ORDER BY pk
SETTINGS auto_statistics_types = 'countmin, basic, tdigest, uniq, uniq_v2', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO tab select number, number, number, toDateTime(number), generateUUIDv4() FROM system.numbers LIMIT 10000;
SELECT column, length(statistics) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active AND column IN ('a', 'b', 'c', 'd', 'pk') ORDER BY column;


SELECT 'Test materialize and clear statistics:';
ALTER TABLE tab CLEAR STATISTICS a, b, c, d;
SELECT column, length(statistics) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active AND column IN ('a', 'b', 'c', 'd', 'pk') ORDER BY column;

ALTER TABLE tab MATERIALIZE STATISTICS b;
SELECT column, length(statistics) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active AND column IN ('a', 'b', 'c', 'd', 'pk') ORDER BY column;

ALTER TABLE tab CLEAR STATISTICS b;
SELECT column, length(statistics) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active AND column IN ('a', 'b', 'c', 'd', 'pk') ORDER BY column;

DROP TABLE IF EXISTS tab SYNC;

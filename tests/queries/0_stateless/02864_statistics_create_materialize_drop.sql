-- Tags: no-fasttest

DROP TABLE IF EXISTS tab SYNC;

SET allow_statistics = 1;
SET use_statistics = 1;
SET allow_suspicious_low_cardinality_types=1;
SET mutations_sync = 2;


SELECT 'Test create statistics:';

CREATE TABLE tab
(
    a LowCardinality(Int64) STATISTICS(countmin, minmax, tdigest, uniq, uniq_v2),
    b LowCardinality(Nullable(String)) STATISTICS(countmin, uniq, uniq_v2),
    c LowCardinality(Nullable(Int64)) STATISTICS(countmin, minmax, tdigest, uniq, uniq_v2),
    d DateTime STATISTICS(countmin, minmax, tdigest, uniq, uniq_v2),
    pk String,
) Engine = MergeTree() ORDER BY pk;

INSERT INTO tab select number, number, number, toDateTime(number), generateUUIDv4() FROM system.numbers LIMIT 10000;
SHOW CREATE TABLE tab;


SELECT 'Test materialize and drop statistics:';
ALTER TABLE tab DROP STATISTICS a, b, c, d;
ALTER TABLE tab ADD STATISTICS b TYPE countmin, uniq, uniq_v2;
ALTER TABLE tab MATERIALIZE STATISTICS b;
SHOW CREATE TABLE tab;

ALTER TABLE tab DROP STATISTICS b;
SHOW CREATE TABLE tab;

DROP TABLE IF EXISTS tab SYNC;

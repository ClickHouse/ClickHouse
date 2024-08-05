-- Tags: no-fasttest

DROP TABLE IF EXISTS tab SYNC;

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET allow_suspicious_low_cardinality_types=1;
SET mutations_sync = 2;


SELECT 'Test create statistics:';

CREATE TABLE tab
(
    a LowCardinality(Int64) STATISTICS(count_min, minmax, uniq),
    b Nullable(Int64),
    c LowCardinality(Nullable(Int64)) STATISTICS(minmax, count_min),
    d DateTime STATISTICS(count_min, minmax, tdigest, uniq),
    pk String,
) Engine = MergeTree() ORDER BY pk;

SHOW CREATE TABLE tab;

SELECT name FROM system.tables WHERE name = 'tab' AND database = currentDatabase();
INSERT INTO tab select number, number, number, toDateTime(number, 'UTC'), generateUUIDv4() FROM system.numbers LIMIT 10000;


SELECT 'Test materialize and drop statistics:';

ALTER TABLE tab ADD STATISTICS b TYPE count_min, minmax, tdigest, uniq;
ALTER TABLE tab MATERIALIZE STATISTICS b;
SHOW CREATE TABLE tab;

ALTER TABLE tab DROP STATISTICS a, b, c, d;
SHOW CREATE TABLE tab;

DROP TABLE IF EXISTS tab SYNC;

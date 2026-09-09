-- Tags: no-parallel-replicas, no-replicated-database
-- The query condition cache is keyed by (table, part name, condition hash), so an entry primed
-- before a pending metadata mutation stays matchable while reads of the column already return
-- the re-added/renamed data. The cache must fail open while mutations are pending.
--
-- Covered: DROP + re-ADD of the same name, and DROP + RENAME into the freed name.
-- Three parts with max_threads = 1: crossing a part boundary forces the reader's buffered
-- mark verdicts into the cache, so the primed entries exist reliably.

SET use_query_condition_cache = 1;
SET optimize_trivial_count_with_sparsity_filter = 0;
-- Statistics-based pruning would drop all parts for the priming predicate without reading them,
-- leaving no verdicts to prime the cache; it is covered by 05045 already.
SET use_statistics_for_part_pruning = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS qcc_readded;
CREATE TABLE qcc_readded (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES qcc_readded;
INSERT INTO qcc_readded VALUES (1, 100);
INSERT INTO qcc_readded VALUES (2, 200);
INSERT INTO qcc_readded VALUES (3, 300);
SELECT 'primed', count() FROM qcc_readded WHERE x = 0;
ALTER TABLE qcc_readded (DROP COLUMN x), (ADD COLUMN x Int64 DEFAULT 0)
    SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'drop+add reads', groupArray(x) FROM qcc_readded;
SELECT 'drop+add x = 0', count() FROM qcc_readded WHERE x = 0;
DROP TABLE qcc_readded;

DROP TABLE IF EXISTS qcc_renamed;
CREATE TABLE qcc_renamed (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES qcc_renamed;
INSERT INTO qcc_renamed VALUES (1, 0);
INSERT INTO qcc_renamed VALUES (2, 0);
INSERT INTO qcc_renamed VALUES (3, 0);
SELECT 'primed', count() FROM qcc_renamed WHERE x = 0;
ALTER TABLE qcc_renamed (DROP COLUMN x), (RENAME COLUMN y TO x)
    SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'drop+rename reads', groupArray(x) FROM qcc_renamed;
SELECT 'drop+rename x = 0', count() FROM qcc_renamed WHERE x = 0;
DROP TABLE qcc_renamed;

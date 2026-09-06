-- The system tables that enumerate tables narrow that enumeration with the query's condition on
-- `database` and on the table name, so a condition that names the tables does not walk every
-- table of every database. Every check below compares a condition the storage pushes down against
-- the same condition applied afterwards inside an aggregate, which it cannot push down: the two
-- must agree, whatever the storage decided to skip.

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_rep;
DROP TABLE IF EXISTS t_detached;
DROP TABLE IF EXISTS t_dist;

CREATE TABLE t_a (k UInt64, v String, INDEX idx_v v TYPE set(100) GRANULARITY 1, CONSTRAINT c_k CHECK k < 1000, PROJECTION p_k (SELECT k ORDER BY k)) ENGINE = MergeTree PARTITION BY k % 2 ORDER BY k;
CREATE TABLE t_b (k UInt64, v String, INDEX idx_v v TYPE set(100) GRANULARITY 1, CONSTRAINT c_k CHECK k < 1000, PROJECTION p_k (SELECT k ORDER BY k)) ENGINE = MergeTree PARTITION BY k % 2 ORDER BY k;
CREATE TABLE t_c (k UInt64, v String, INDEX idx_v v TYPE set(100) GRANULARITY 1, CONSTRAINT c_k CHECK k < 1000, PROJECTION p_k (SELECT k ORDER BY k)) ENGINE = MergeTree PARTITION BY k % 2 ORDER BY k;
CREATE TABLE t_rep (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rep', 'r1') ORDER BY k;
CREATE TABLE t_detached (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dist (k UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_a, k);

INSERT INTO t_a SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_b SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_c SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_rep SELECT number FROM numbers(10);

ALTER TABLE t_a DELETE WHERE k = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_b DELETE WHERE k = 1 SETTINGS mutations_sync = 2;

ALTER TABLE t_a DETACH PARTITION 1;
ALTER TABLE t_c DETACH PARTITION 1;

DETACH TABLE t_detached PERMANENTLY;

SELECT '-- system.tables';
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t_a', 't_c') ORDER BY name;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t_a', 'no_such_table') ORDER BY name;
SELECT name FROM system.tables WHERE database IN (currentDatabase(), 'no_such_database') AND name IN ('t_b') ORDER BY name;
-- `table` is an alias of `name`.
SELECT name FROM system.tables WHERE database = currentDatabase() AND table IN ('t_a', 't_b') ORDER BY name;
-- The names come from a set that is only built at execution time.
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN (SELECT 't_' || arrayJoin(['a', 'c'])) ORDER BY name;
-- Shapes that pin nothing down must still return everything they should.
SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't\_%' AND name NOT IN ('t_a', 't_b') ORDER BY name;
SELECT name FROM system.tables WHERE database = currentDatabase() AND (name IN ('t_a') OR name LIKE 't\_c') ORDER BY name;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't\_%' AND (name IN ('t_a') OR length(name) = 3) ORDER BY name;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name != 't_a' AND name IN ('t_a', 't_b') ORDER BY name;
SELECT name FROM system.tables WHERE database = currentDatabase() AND (database, name) IN ((currentDatabase(), 't_a')) ORDER BY name;
-- An unsatisfiable condition selects nothing rather than everything.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_a') AND name IN ('t_b');

SELECT '-- pushed down == applied afterwards';
WITH ['t_a', 't_c'] AS wanted
SELECT
    'tables',
    (SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, name)) FROM system.tables WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'columns',
    (SELECT count() FROM system.columns WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.columns WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'parts',
    (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.parts WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'parts_columns',
    (SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.parts_columns WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'projection_parts',
    (SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.projection_parts WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'detached_parts',
    (SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.detached_parts WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'data_skipping_indices',
    (SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.data_skipping_indices WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'projections',
    (SELECT count() FROM system.projections WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.projections WHERE database = currentDatabase());

WITH ['t_a', 't_c'] AS wanted
SELECT
    'constraints',
    (SELECT count() FROM system.constraints WHERE database = currentDatabase() AND table IN ('t_a', 't_c'))
        = (SELECT countIf(has(wanted, table)) FROM system.constraints WHERE database = currentDatabase());

WITH ['t_a', 't_b'] AS wanted
SELECT
    'mutations',
    (SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table IN ('t_a', 't_b'))
        = (SELECT countIf(has(wanted, table)) FROM system.mutations WHERE database = currentDatabase());

WITH ['t_rep'] AS wanted
SELECT
    'replicas',
    (SELECT count() FROM system.replicas WHERE database = currentDatabase() AND table IN ('t_rep'))
        = (SELECT countIf(has(wanted, table)) FROM system.replicas WHERE database = currentDatabase());

WITH ['t_rep'] AS wanted
SELECT
    'replication_queue',
    (SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table IN ('t_rep'))
        = (SELECT countIf(has(wanted, table)) FROM system.replication_queue WHERE database = currentDatabase());

WITH ['t_rep'] AS wanted
SELECT
    'part_moves_between_shards',
    (SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table IN ('t_rep'))
        = (SELECT countIf(has(wanted, table)) FROM system.part_moves_between_shards WHERE database = currentDatabase());

WITH ['t_dist'] AS wanted
SELECT
    'distribution_queue',
    (SELECT count() FROM system.distribution_queue WHERE database = currentDatabase() AND table IN ('t_dist'))
        = (SELECT countIf(has(wanted, table)) FROM system.distribution_queue WHERE database = currentDatabase());

WITH ['t_detached'] AS wanted
SELECT
    'detached_tables',
    (SELECT count() FROM system.detached_tables WHERE database = currentDatabase() AND table IN ('t_detached'))
        = (SELECT countIf(has(wanted, table)) FROM system.detached_tables WHERE database = currentDatabase());

SELECT '-- system.replicas rows are complete';
SELECT table, is_readonly FROM system.replicas WHERE database = currentDatabase() AND table IN ('t_rep');

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_rep;
DROP TABLE t_dist;

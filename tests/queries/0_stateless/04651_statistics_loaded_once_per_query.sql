-- Tags: no-random-merge-tree-settings, no-parallel-replicas

-- The stress profile gives a fifth of its workers a random `compatibility` version, which rolls back
-- whatever settings that version predates, among them the plan renderer that prints the join symbol
-- asserted below and the join-order search this measures. Everything here is current-version planning.
SET compatibility = '';
SET allow_statistics = 1;
SET enable_analyzer = 1;
SET use_statistics = 1;
SET async_insert = 0;
-- Pinned to the shipped defaults, which `clickhouse-test` otherwise randomizes.
SET materialize_statistics_on_insert = 1;
SET materialize_statistics_on_insert_max_table_size = 26843545600;
-- Condition reordering is what asks for the estimator, and it needs both of these: either one off
-- takes prewhere out of the picture and nothing loads statistics at all.
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
-- `ast_fuzzer_runs` is pinned because the stress profile enables the server-side AST fuzzer for any
-- query, including the inserts below: a fuzzed re-execution of one writes further parts, and every
-- byte count here is read against a fixed part set.
SET ast_fuzzer_runs = 0;

DROP TABLE IF EXISTS t_stats_once_a SYNC;
DROP TABLE IF EXISTS t_stats_once_b SYNC;
DROP TABLE IF EXISTS t_stats_once_c SYNC;
DROP TABLE IF EXISTS t_stats_once_p SYNC;

-- `refresh_statistics_interval = 0` disables the background task that publishes the table-wide
-- estimator cache, so every query below takes the per-query path under test instead of racing
-- that task for it.
CREATE TABLE t_stats_once_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS refresh_statistics_interval = 0, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_stats_once_b (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS refresh_statistics_interval = 0, auto_statistics_types = 'basic, uniq_v2';
-- Declared `aa` first, so a caller naming the columns in declaration order and one naming them in
-- reference order disagree about the order of the same set.
CREATE TABLE t_stats_once_c (aa UInt64, k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS refresh_statistics_interval = 0, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_stats_once_p (part UInt8, k UInt64, v UInt64) ENGINE = MergeTree
PARTITION BY part ORDER BY k
SETTINGS refresh_statistics_interval = 0, auto_statistics_types = 'basic, uniq_v2';

SYSTEM STOP MERGES t_stats_once_a;
SYSTEM STOP MERGES t_stats_once_b;
SYSTEM STOP MERGES t_stats_once_c;
SYSTEM STOP MERGES t_stats_once_p;

-- Four parts each, so one load reads four sets of statistics files and the byte count is well clear
-- of a single part's. `a` and `b` get the same four part names and differ only in row count, which
-- is what makes the table-identity check below discriminating.
INSERT INTO t_stats_once_a SELECT number, number FROM numbers(25000);
INSERT INTO t_stats_once_a SELECT number, number FROM numbers(25000);
INSERT INTO t_stats_once_a SELECT number, number FROM numbers(25000);
INSERT INTO t_stats_once_a SELECT number, number FROM numbers(25000);
INSERT INTO t_stats_once_b SELECT number, number FROM numbers(175);
INSERT INTO t_stats_once_b SELECT number, number FROM numbers(175);
INSERT INTO t_stats_once_b SELECT number, number FROM numbers(175);
INSERT INTO t_stats_once_b SELECT number, number FROM numbers(175);
INSERT INTO t_stats_once_c SELECT number, number, number FROM numbers(25000);
INSERT INTO t_stats_once_c SELECT number, number, number FROM numbers(25000);
INSERT INTO t_stats_once_c SELECT number, number, number FROM numbers(25000);
INSERT INTO t_stats_once_c SELECT number, number, number FROM numbers(25000);
-- The two partitions differ in row count, so a relation estimated from the wrong one reports a
-- visibly wrong number of rows.
INSERT INTO t_stats_once_p SELECT 1, number, number FROM numbers(25000);
INSERT INTO t_stats_once_p SELECT 2, number, number FROM numbers(700);

SELECT 'statistics on disk', count() > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_once_a' AND active AND length(statistics) > 0;

-- Both tables must carry the same part names for the table-identity check further down to be about
-- table identity rather than about part-set size.
SELECT 'same part names', countDistinct(names) = 1 FROM (
    SELECT table, arraySort(groupArray(name)) AS names FROM system.parts
    WHERE database = currentDatabase() AND table IN ('t_stats_once_a', 't_stats_once_b') AND active
    GROUP BY table
);

-- Every branch reads the same table with the same required columns, so each asks for an estimator
-- over the same part set. The keys match no row, which keeps `read_rows` at 0 so `ReadCompressedBytes`
-- accounts for statistics alone; each branch has two conditions on `v` so condition reordering asks
-- for the estimator in the first place.
SELECT count() FROM (
    SELECT k FROM t_stats_once_a WHERE k = 900000001 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000002 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_n2', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

SELECT count() FROM (
    SELECT k FROM t_stats_once_a WHERE k = 900000001 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000002 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000003 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000004 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000005 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000006 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000007 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000008 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_n8', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

-- `use_statistics_cache = 0` means "do not reuse", so this arm keeps loading per branch. It is also
-- what shows the reuse above is gated on the setting rather than unconditional.
SELECT count() FROM (
    SELECT k FROM t_stats_once_a WHERE k = 900000001 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000002 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000003 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000004 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000005 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000006 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000007 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000008 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_n8_nocache', use_statistics_cache = 0, ast_fuzzer_runs = 0 FORMAT Null;

-- Room for one entry does not disturb branches that all want the same one: this is the control for
-- the two arms below, which are what shows the limit is enforced.
SELECT count() FROM (
    SELECT k FROM t_stats_once_a WHERE k = 900000001 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000002 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000003 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000004 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000005 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000006 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000007 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000008 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_n8_cap1', use_statistics_cache = 1, statistics_cache_max_entries = 1,
    ast_fuzzer_runs = 0 FORMAT Null;

-- A zero limit reaches the same outcome as turning reuse off, so the limit alone can restore the
-- old behaviour.
SELECT count() FROM (
    SELECT k FROM t_stats_once_a WHERE k = 900000001 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000002 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000003 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000004 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000005 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000006 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000007 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_a WHERE k = 900000008 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_n8_cap0', use_statistics_cache = 1, statistics_cache_max_entries = 0,
    ast_fuzzer_runs = 0 FORMAT Null;

-- Room for one entry against six branches wanting two: the branches asking for whichever set arrives
-- second find no room and load again, so this reads more than the same query does with room for both.
SELECT count() FROM (
    SELECT k, aa FROM t_stats_once_c WHERE k = 900000101 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, w FROM t_stats_once_c WHERE k = 900000102 AND w >= 0 AND w < 100000
    UNION ALL SELECT k, aa FROM t_stats_once_c WHERE k = 900000103 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, w FROM t_stats_once_c WHERE k = 900000104 AND w >= 0 AND w < 100000
    UNION ALL SELECT k, aa FROM t_stats_once_c WHERE k = 900000105 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, w FROM t_stats_once_c WHERE k = 900000106 AND w >= 0 AND w < 100000
) SETTINGS log_comment = '04651_alt_cap1', use_statistics_cache = 1, statistics_cache_max_entries = 1,
    ast_fuzzer_runs = 0 FORMAT Null;

SELECT count() FROM (
    SELECT k, aa FROM t_stats_once_c WHERE k = 900000101 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, w FROM t_stats_once_c WHERE k = 900000102 AND w >= 0 AND w < 100000
    UNION ALL SELECT k, aa FROM t_stats_once_c WHERE k = 900000103 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, w FROM t_stats_once_c WHERE k = 900000104 AND w >= 0 AND w < 100000
    UNION ALL SELECT k, aa FROM t_stats_once_c WHERE k = 900000105 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, w FROM t_stats_once_c WHERE k = 900000106 AND w >= 0 AND w < 100000
) SETTINGS log_comment = '04651_alt_default', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The load happens; it happens once however many branches ask for it; it happens per branch again
-- once reuse is turned off, and likewise once the limit leaves no room to reuse.
SELECT
    'loaded once for any branch count',
    maxIf(bytes, log_comment = '04651_n2') > 0 AND maxIf(loaded, log_comment = '04651_n2') = 1,
    maxIf(bytes, log_comment = '04651_n8') = maxIf(bytes, log_comment = '04651_n2'),
    maxIf(bytes, log_comment = '04651_n8_nocache') = 8 * maxIf(bytes, log_comment = '04651_n2'),
    maxIf(bytes, log_comment = '04651_n8_cap1') = maxIf(bytes, log_comment = '04651_n2'),
    maxIf(bytes, log_comment = '04651_n8_cap0') = maxIf(bytes, log_comment = '04651_n8_nocache'),
    max(rows_read) = 0
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['ReadCompressedBytes'], event_time_microseconds) AS bytes,
        argMax(ProfileEvents['LoadedStatisticsMicroseconds'] > 0, event_time_microseconds) AS loaded,
        argMax(read_rows, event_time_microseconds) AS rows_read
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('04651_n2', '04651_n8', '04651_n8_nocache', '04651_n8_cap1', '04651_n8_cap0')
    GROUP BY log_comment
);

-- The limit is enforced rather than merely accepted: six branches over two sets read one set's
-- statistics twice more with room for one entry than with room for both, because the branches wanting
-- the set that arrives second keep finding no room. Two of the six do the reloading, hence 4 loads
-- against 2.
SELECT
    'the limit is enforced',
    maxIf(bytes, log_comment = '04651_alt_default') = 2 * maxIf(bytes, log_comment = '04651_n2'),
    maxIf(bytes, log_comment = '04651_alt_cap1') = 2 * maxIf(bytes, log_comment = '04651_alt_default'),
    max(rows_read) = 0
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['ReadCompressedBytes'], event_time_microseconds) AS bytes,
        argMax(read_rows, event_time_microseconds) AS rows_read
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('04651_n2', '04651_alt_cap1', '04651_alt_default')
    GROUP BY log_comment
);

-- The names of the required columns denote a set: the loader reads them as one, so two branches
-- naming `aa` and `k` in opposite order ask for the same statistics and one load answers both. The
-- third branch names a different set and loads separately, which is what keeps the first comparison
-- from being satisfied by sharing everything with everything.
SELECT count() FROM (
    SELECT aa, k FROM t_stats_once_c WHERE aa >= 0 AND aa < 100000 AND k = 900000001
    UNION ALL SELECT k, aa FROM t_stats_once_c WHERE k = 900000002 AND aa >= 0 AND aa < 100000
) SETTINGS log_comment = '04651_order', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

SELECT count() FROM (
    SELECT k, aa FROM t_stats_once_c WHERE k = 900000003 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, aa FROM t_stats_once_c WHERE k = 900000004 AND aa >= 0 AND aa < 100000
) SETTINGS log_comment = '04651_order_same', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

SELECT count() FROM (
    SELECT k, aa FROM t_stats_once_c WHERE k = 900000005 AND aa >= 0 AND aa < 100000
    UNION ALL SELECT k, w FROM t_stats_once_c WHERE k = 900000006 AND w >= 0 AND w < 100000
) SETTINGS log_comment = '04651_sets', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

-- A branch naming the partition column asks about one more column than a branch that does not, so
-- the two are not answered from the same statistics. Partition pruning does not narrow the part set
-- here, because this path builds the estimator before range analysis: both branches carry the whole
-- set. Pruned part sets reach the key only on the join path, covered further down.
SELECT count() FROM (
    SELECT k FROM t_stats_once_p WHERE part = 1 AND k = 900000001 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_p WHERE k = 900000002 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_parts', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

SELECT count() FROM (
    SELECT k FROM t_stats_once_p WHERE k = 900000003 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_p WHERE k = 900000004 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_parts_same', use_statistics_cache = 1, ast_fuzzer_runs = 0 FORMAT Null;

SELECT count() FROM (
    SELECT k FROM t_stats_once_p WHERE k = 900000003 AND v >= 0 AND v < 100000
    UNION ALL SELECT k FROM t_stats_once_p WHERE k = 900000004 AND v >= 0 AND v < 100000
) SETTINGS log_comment = '04651_parts_same_nocache', use_statistics_cache = 0, ast_fuzzer_runs = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    'keyed on the column set',
    -- The comparisons below are between byte counts and would all hold at zero, so one arm is
    -- required to have loaded something.
    maxIf(bytes, log_comment = '04651_order_same') > 0,
    maxIf(bytes, log_comment = '04651_order') = maxIf(bytes, log_comment = '04651_order_same'),
    maxIf(bytes, log_comment = '04651_sets') = 2 * maxIf(bytes, log_comment = '04651_order_same'),
    maxIf(bytes, log_comment = '04651_parts_same_nocache') = 2 * maxIf(bytes, log_comment = '04651_parts_same'),
    maxIf(bytes, log_comment = '04651_parts') > maxIf(bytes, log_comment = '04651_parts_same'),
    max(rows_read) = 0
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['ReadCompressedBytes'], event_time_microseconds) AS bytes,
        argMax(read_rows, event_time_microseconds) AS rows_read
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('04651_order', '04651_order_same', '04651_sets', '04651_parts',
            '04651_parts_same', '04651_parts_same_nocache')
    GROUP BY log_comment
);

-- Reuse is keyed on the table, not on part names alone: the two tables carry the same four part
-- names and differ only in row count, so each relation must still report its own.
-- `query_plan_optimize_join_order_randomize = 0` because a non-zero value replaces the estimates
-- printed here with random ones, and `query_plan_join_swap_table = 'false'` because a swap prints the
-- two relations the other way round. Both are randomized by the test runner and the stress profile,
-- so every join query measured here pins them.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM t_stats_once_a AS a JOIN t_stats_once_b AS b ON a.k = b.k
    WHERE a.v >= 0 AND b.v >= 0
    SETTINGS use_statistics_cache = 1, query_plan_optimize_join_order_limit = 10,
        query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_algorithm = 'greedy',
        query_plan_join_swap_table = 'false', ast_fuzzer_runs = 0
) WHERE explain LIKE '%⋈%';

-- The limit bounds how much is held, never what is estimated.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM t_stats_once_a AS a JOIN t_stats_once_b AS b ON a.k = b.k
    WHERE a.v >= 0 AND b.v >= 0
    SETTINGS use_statistics_cache = 1, statistics_cache_max_entries = 1,
        query_plan_optimize_join_order_limit = 10,
        query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_algorithm = 'greedy',
        query_plan_join_swap_table = 'false', ast_fuzzer_runs = 0
) WHERE explain LIKE '%⋈%';

-- Reuse is keyed on which parts a branch reads, not on how many: two relations pruned to one
-- partition each read the same number of parts, so only the part names tell their statistics apart.
-- Estimates are compared rather than bytes, because a relation answered from the wrong partition
-- reports that partition's row count.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS pd_a
    JOIN (SELECT k, v FROM t_stats_once_p WHERE part = 2) AS pd_b ON pd_a.k = pd_b.k
    WHERE pd_a.v >= 0 AND pd_b.v >= 0
    SETTINGS use_statistics_cache = 1, query_plan_optimize_join_order_limit = 10,
        query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_algorithm = 'greedy',
        query_plan_join_swap_table = 'false', ast_fuzzer_runs = 0
) WHERE explain LIKE '%⋈%';

-- The same two partitions with reuse turned off, which is what pruning alone reports: a disagreement
-- between this arm and the one above is about reuse rather than about pruning.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS pn_a
    JOIN (SELECT k, v FROM t_stats_once_p WHERE part = 2) AS pn_b ON pn_a.k = pn_b.k
    WHERE pn_a.v >= 0 AND pn_b.v >= 0
    SETTINGS use_statistics_cache = 0, query_plan_optimize_join_order_limit = 10,
        query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_algorithm = 'greedy',
        query_plan_join_swap_table = 'false', ast_fuzzer_runs = 0
) WHERE explain LIKE '%⋈%';

-- Two relations over the same partition ask about the same parts, so one answers both.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT count() FROM (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS ps_a
    JOIN (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS ps_b ON ps_a.k = ps_b.k
    WHERE ps_a.v >= 0 AND ps_b.v >= 0
    SETTINGS use_statistics_cache = 1, query_plan_optimize_join_order_limit = 10,
        query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_algorithm = 'greedy',
        query_plan_join_swap_table = 'false', ast_fuzzer_runs = 0
) WHERE explain LIKE '%⋈%';

-- The arms above assert what the join path estimates; these two assert that it consults the reuse at
-- all. Three callers ask about the same single partition, so all three want one entry: with reuse the
-- first loads and the other two are answered, without it each loads. The three are condition
-- reordering once per relation and the runtime filter's own read, so `enable_join_runtime_filters`
-- is pinned, and `join_algorithm` with it: the filter is attached only when the algorithm list names
-- a hash-family algorithm. With either unpinned there are two callers and the ratio below is 2
-- rather than 3.
-- A throwing row limit makes join-order estimation analyze ranges without memoizing the result, so a
-- caller that reads that memo falls back to the whole part set and asks about a second entry the
-- ratio does not describe: `max_rows_to_read` and its leaf twin are pinned to the shipped default,
-- which the test profile raises. With either set, one of the three asks about both partitions.
-- The `EXPLAIN` is wrapped in a counting query, so no plan text reaches the output; `FORMAT Null` on
-- its own does not suppress it.
SELECT count() FROM (
    EXPLAIN SELECT count() FROM (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS ja_a
    JOIN (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS ja_b ON ja_a.k = ja_b.k
    WHERE ja_a.v >= 0 AND ja_b.v >= 0
) SETTINGS log_comment = '04651_join_same', use_statistics_cache = 1,
    enable_join_runtime_filters = 1, join_algorithm = 'hash',
    max_rows_to_read = 0, max_rows_to_read_leaf = 0,
    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
    query_plan_optimize_join_order_algorithm = 'greedy', query_plan_join_swap_table = 'false',
    ast_fuzzer_runs = 0 FORMAT Null;

SELECT count() FROM (
    EXPLAIN SELECT count() FROM (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS jn_a
    JOIN (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS jn_b ON jn_a.k = jn_b.k
    WHERE jn_a.v >= 0 AND jn_b.v >= 0
) SETTINGS log_comment = '04651_join_same_nocache', use_statistics_cache = 0,
    enable_join_runtime_filters = 1, join_algorithm = 'hash',
    max_rows_to_read = 0, max_rows_to_read_leaf = 0,
    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
    query_plan_optimize_join_order_algorithm = 'greedy', query_plan_join_swap_table = 'false',
    ast_fuzzer_runs = 0 FORMAT Null;

-- Nothing is estimated with statistics turned off, so this arm reads none and is what says the byte
-- counts above are statistics rather than the plan text the two queries also produce.
SELECT count() FROM (
    EXPLAIN SELECT count() FROM (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS jz_a
    JOIN (SELECT k, v FROM t_stats_once_p WHERE part = 1) AS jz_b ON jz_a.k = jz_b.k
    WHERE jz_a.v >= 0 AND jz_b.v >= 0
) SETTINGS log_comment = '04651_join_same_nostat', use_statistics = 0,
    enable_join_runtime_filters = 1, join_algorithm = 'hash',
    max_rows_to_read = 0, max_rows_to_read_leaf = 0,
    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
    query_plan_optimize_join_order_algorithm = 'greedy', query_plan_join_swap_table = 'false',
    ast_fuzzer_runs = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    'the join path reuses',
    maxIf(bytes, log_comment = '04651_join_same') > 0,
    maxIf(bytes, log_comment = '04651_join_same_nocache') = 3 * maxIf(bytes, log_comment = '04651_join_same'),
    maxIf(bytes, log_comment = '04651_join_same_nostat') = 0
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['ReadCompressedBytes'], event_time_microseconds) AS bytes
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('04651_join_same', '04651_join_same_nocache', '04651_join_same_nostat')
    GROUP BY log_comment
);

-- Exactly one row per measured query: the comparisons above are between byte counts, so a second row
-- under the same `log_comment` would let them compare unrelated numbers, and a missing one reads as
-- zero bytes there rather than as an error - hence the group count is asserted too, since an absent
-- `log_comment` leaves no group to inspect. The comments are listed rather than matched by prefix,
-- because the test runner labels the whole file with one of its own.
SELECT 'one row per query', count() = 16, max(rows_logged) = 1 FROM (
    SELECT log_comment, count() AS rows_logged FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('04651_n2', '04651_n8', '04651_n8_nocache', '04651_n8_cap1', '04651_n8_cap0',
            '04651_alt_cap1', '04651_alt_default',
            '04651_order', '04651_order_same', '04651_sets', '04651_parts', '04651_parts_same',
            '04651_parts_same_nocache',
            '04651_join_same', '04651_join_same_nocache', '04651_join_same_nostat')
    GROUP BY log_comment
);

DROP TABLE t_stats_once_a SYNC;
DROP TABLE t_stats_once_b SYNC;
DROP TABLE t_stats_once_c SYNC;
DROP TABLE t_stats_once_p SYNC;

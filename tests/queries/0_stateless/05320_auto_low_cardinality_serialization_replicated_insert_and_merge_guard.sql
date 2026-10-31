-- `INSERT SELECT` through `ARRAY JOIN` and `JOIN` can bring lazily replicated `String` columns to the
-- writer. Building the dictionary of an automatically `LowCardinality`-encoded column must strip that
-- wrapper first.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_lazy_columns_replication = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_auto_lc_replicated_src;
DROP TABLE IF EXISTS t_auto_lc_replicated_dst;
DROP TABLE IF EXISTS t_auto_lc_replicated_plain;
DROP TABLE IF EXISTS t_auto_lc_replicated_merge;

CREATE TABLE t_auto_lc_replicated_src (id UInt64, s String, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_auto_lc_replicated_src SELECT number, 'v_' || toString(number % 10), range(number % 5 + 1) FROM numbers(1000);

CREATE TABLE t_auto_lc_replicated_dst
(
    id UInt64,
    s String STATISTICS(uniq),
    fs FixedString(4) STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_replicated_dst SELECT id * 10 + x, s, toFixedString(substring(s, 1, 4), 4) FROM t_auto_lc_replicated_src ARRAY JOIN arr AS x;

INSERT INTO t_auto_lc_replicated_dst
SELECT l.id * 10 + r.number, l.s, toFixedString(substring(l.s, 1, 4), 4)
FROM t_auto_lc_replicated_src AS l INNER JOIN numbers(3) AS r ON l.id % 3 = r.number;

SELECT 'kinds', column, groupUniqArray(serialization_kind)
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_replicated_dst' AND active AND column IN ('s', 'fs')
GROUP BY column ORDER BY column;

SELECT 'values', count(), uniqExact(s), uniqExact(fs), sum(length(s)) FROM t_auto_lc_replicated_dst;

-- A `Merge` table enumerates its matching tables again when it reads, so a table that encodes the
-- column can join the match set after the query has been analyzed. The rewrite of `length` of a
-- `String` column to its `.size` subcolumn is therefore skipped even when no current match encodes it.
CREATE TABLE t_auto_lc_replicated_plain (id UInt64, s String, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_auto_lc_replicated_plain VALUES (1, 'abc', [1, 2]);
CREATE TABLE t_auto_lc_replicated_merge AS t_auto_lc_replicated_plain ENGINE = Merge(currentDatabase(), '^t_auto_lc_replicated_plain$');

SELECT 'merge, String rewrite skipped', count()
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_replicated_merge SETTINGS optimize_functions_to_subcolumns = 1)
WHERE explain LIKE '%s.size%';

SELECT 'merge, Array rewrite still fires', count() > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(arr) FROM t_auto_lc_replicated_merge SETTINGS optimize_functions_to_subcolumns = 1)
WHERE explain LIKE '%arr.size0%';

SELECT 'merge, values', length(s), length(arr) FROM t_auto_lc_replicated_merge;

DROP TABLE t_auto_lc_replicated_merge;
DROP TABLE t_auto_lc_replicated_plain;
DROP TABLE t_auto_lc_replicated_dst;
DROP TABLE t_auto_lc_replicated_src;

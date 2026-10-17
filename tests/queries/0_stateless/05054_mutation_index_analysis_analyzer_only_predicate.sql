-- Tags: no-old-analyzer
-- The index-analysis shortcut for untouched mutation parts must resolve the predicate with the
-- analyzer that will interpret the mutation. An `ALIAS` column resolves only under the query-tree
-- analyzer, so a predicate over one used to miss the shortcut entirely and fall back to running
-- the `SELECT count()` check query on every part.

DROP TABLE IF EXISTS t_mutation_index_analysis_alias;

CREATE TABLE t_mutation_index_analysis_alias (id UInt64, v UInt64, doubled UInt64 ALIAS id * 2)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_mutation_index_analysis_alias (id, v) SELECT number, 0 FROM numbers(100);
INSERT INTO t_mutation_index_analysis_alias (id, v) SELECT 1000 + number, 0 FROM numbers(100);

ALTER TABLE t_mutation_index_analysis_alias UPDATE v = 1 WHERE doubled >= 1000000 SETTINGS mutations_sync = 2;

SELECT sum(v), count() FROM t_mutation_index_analysis_alias;

SYSTEM FLUSH LOGS part_log;

-- Both parts are proven untouched from the primary key index alone.
SELECT sum(ProfileEvents['MutationUntouchedPartsByIndexAnalysis'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_mutation_index_analysis_alias' AND event_type = 'MutatePart';

DROP TABLE t_mutation_index_analysis_alias;

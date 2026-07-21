-- Tags: no-random-merge-tree-settings
-- Regression: a mixed structural + settings + comment ALTER on a ReplicatedMergeTree reverted the
-- setting-derived escape_index_filenames metadata field (and per-index escape_filenames). The bundled
-- comment commit in the full replicated-ALTER path used a metadata snapshot captured before
-- changeSettings, so the new index-filename policy was silently reverted: a part written after the
-- ALTER got the OLD (unescaped) index file names while the metadata (re-derived from the .sql on
-- reload) expects the NEW (escaped) names, leaving the part's skip index silently dead.

DROP TABLE IF EXISTS t_escape_revert;

CREATE TABLE t_escape_revert
(
    k UInt64,
    s String,
    INDEX `i.dx` s TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_escape_revert', 'r1')
ORDER BY k
SETTINGS escape_index_filenames = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- Mixed ALTER: a structural change routes it to the full replicated-ALTER path, which also applies the
-- bundled settings/comment change locally. MODIFY COMMENT must precede MODIFY SETTING (the parser
-- otherwise consumes the comma).
ALTER TABLE t_escape_revert
    ADD COLUMN x UInt8 DEFAULT 0, MODIFY COMMENT 'c', MODIFY SETTING escape_index_filenames = 1;

-- This part is written after the flip, so it must honor the new (escaped) index-filename policy.
INSERT INTO t_escape_revert SELECT number, toString(number), 0 FROM numbers(1000);

-- Re-derive the in-memory metadata from the .sql (escape_index_filenames = 1), like a server restart.
DETACH TABLE t_escape_revert;
ATTACH TABLE t_escape_revert;

-- The post-ALTER part's skip index must still be usable: a value absent from the part is pruned to
-- 0/1. Without the fix the index files were written unescaped while the reloaded metadata expects
-- escaped names, so the index is silently dead and nothing is pruned (no "Parts: 0/1" line).
SELECT countIf(explain LIKE '%Parts: 0/1%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_escape_revert WHERE s = 'nonexistent');

-- The data itself is intact regardless.
SELECT count() FROM t_escape_revert;

DROP TABLE t_escape_revert;

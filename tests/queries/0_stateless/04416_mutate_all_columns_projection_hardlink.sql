-- Tags: no-fasttest
-- An all-columns mutation (rewriting a Dynamic column) hardlinks an untouched materialized
-- PROJECTION from the source part instead of rebuilding it. The rebuilt checksums.txt must track
-- the hardlinked projection (<name>.proj), otherwise CHECK TABLE fails and the projection is unusable.
SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS t_proj_hardlink;

CREATE TABLE t_proj_hardlink (k UInt64, b UInt64, c UInt64, d Dynamic,
    PROJECTION p (SELECT b, sum(c) GROUP BY b))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0,               -- Wide+Full source: projection is hardlinked,
         min_bytes_for_full_part_storage = 0;       -- not rebuilt

INSERT INTO t_proj_hardlink SELECT number, number % 4, number, number::Dynamic FROM numbers(1000);
ALTER TABLE t_proj_hardlink MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

-- All-columns path (Dynamic column); projection depends only on (b, c) -> untouched -> hardlinked
ALTER TABLE t_proj_hardlink UPDATE d = (k + 1)::Dynamic WHERE 1 SETTINGS mutations_sync = 2;

CHECK TABLE t_proj_hardlink SETTINGS check_query_single_value_result = 1;
SELECT 'projection_parts', count() FROM system.projection_parts
    WHERE database = currentDatabase() AND table = 't_proj_hardlink' AND name = 'p' AND active;
SELECT b, sum(c) FROM t_proj_hardlink GROUP BY b ORDER BY b SETTINGS force_optimize_projection = 1;

DROP TABLE t_proj_hardlink;

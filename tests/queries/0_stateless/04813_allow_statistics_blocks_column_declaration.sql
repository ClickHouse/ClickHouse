-- Verify that allow_statistics = 0 also blocks the column-declaration spelling
-- ALTER TABLE ... ADD/MODIFY COLUMN ... STATISTICS(...), like it blocks the
-- dedicated ADD/DROP/MODIFY STATISTICS commands.
SET allow_statistics = 1;

DROP TABLE IF EXISTS t_allow_statistics_col_decl;
CREATE TABLE t_allow_statistics_col_decl (x UInt32) ENGINE = MergeTree ORDER BY x;

SET allow_statistics = 0;

ALTER TABLE t_allow_statistics_col_decl ADD COLUMN s UInt64 STATISTICS(tdigest); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_allow_statistics_col_decl MODIFY COLUMN x UInt32 STATISTICS(tdigest); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_allow_statistics_col_decl MODIFY COLUMN x COMMENT 'gated' STATISTICS(tdigest); -- { serverError INCORRECT_QUERY }
ALTER TABLE t_allow_statistics_col_decl MODIFY COLUMN x STATISTICS(tdigest) COMMENT 'gated'; -- { serverError INCORRECT_QUERY }

SET allow_statistics = 1;

ALTER TABLE t_allow_statistics_col_decl ADD COLUMN s UInt64 STATISTICS(tdigest);
ALTER TABLE t_allow_statistics_col_decl MODIFY COLUMN x UInt32 STATISTICS(uniq);
SHOW CREATE TABLE t_allow_statistics_col_decl FORMAT TSVRaw;

DROP TABLE t_allow_statistics_col_decl;

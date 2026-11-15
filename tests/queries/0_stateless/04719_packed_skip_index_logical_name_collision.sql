-- Tags: no-darwin, no-random-merge-tree-settings
-- macOS filesystem (APFS) is case-insensitive, so MergeTree hashes stream filenames
-- unconditionally (replace_long_file_name_to_hash = 0 cannot be honored). The arms below need the
-- column to keep its unhashed name: they cover producers that claim an index's LOGICAL name, which
-- can only meet a column whose on-disk name is logical too. Under forced hashing the column becomes
-- bare hex, the pair is genuinely legal, and the refusals below correctly stop happening.
-- The rest of the matrix hashes both sides to one base and stays platform-agnostic in 04698.

SET mutations_sync = 2;

-- A substream that stays inside `skp_idx.packed` still owns the base: reads resolve `skp_idx_*`
-- archive keys before the real disk, so the archive member shadows the column's own file.
CREATE TABLE t_packed (k UInt64, `skp_idx_a` UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         packed_skip_index_max_bytes = 1000000;
INSERT INTO t_packed SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_packed;

-- Building the column before the index exists leaves the merge as the only producer that can see
-- the pair, which is the claim inside MergeTextIndexesTask.
CREATE TABLE t_text_merge (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_text_merge SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_text_merge SELECT number + 100, toString(number) FROM numbers(10);
ALTER TABLE t_text_merge ADD COLUMN `skp_idx_a` String DEFAULT 'x';
ALTER TABLE t_text_merge UPDATE `skp_idx_a` = 'y' WHERE 1;
ALTER TABLE t_text_merge ADD INDEX a(s) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE t_text_merge FINAL; -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_text_merge;

-- A carried archive member is claimed under its logical name, which is what a read resolves.
CREATE TABLE t_carry_packed (k UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         packed_skip_index_max_bytes = 1000000;
INSERT INTO t_carry_packed SELECT number, toString(number) FROM numbers(10);
ALTER TABLE t_carry_packed ADD COLUMN `skp_idx_a` UInt64 DEFAULT 7;
ALTER TABLE t_carry_packed UPDATE `skp_idx_a` = 5 WHERE 1; -- { serverError UNFINISHED }
SELECT 't_carry_packed', countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%'
    AND latest_fail_reason LIKE '%skip index `a`%'
    AND latest_fail_reason LIKE '%column `skp_idx_a`%')
FROM system.mutations WHERE database = currentDatabase() AND table = 't_carry_packed';
DROP TABLE t_carry_packed;

-- A persisted table over `mergeTreeTextIndex` would resolve the source table without a user.
CREATE TABLE tab (s String, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tab_index AS mergeTreeTextIndex(currentDatabase(), tab, idx_s); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_index (part_name String, token String) AS mergeTreeTextIndex(currentDatabase(), tab, idx_s); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tab_index';

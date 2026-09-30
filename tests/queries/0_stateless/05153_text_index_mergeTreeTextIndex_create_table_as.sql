-- A persisted definition over `mergeTreeTextIndex`, directly or nested in `remote`, would resolve the source table
-- without the reader's grants. The refusal happens before the structure is inferred, so port 1 is never contacted.
CREATE TABLE tab (s String, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tab_index AS mergeTreeTextIndex(currentDatabase(), tab, idx_s); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_index (part_name String, token String) AS mergeTreeTextIndex(currentDatabase(), tab, idx_s); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_index AS remote('127.0.0.1:1', mergeTreeTextIndex(currentDatabase(), tab, idx_s)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_index (token String) ENGINE = Remote('127.0.0.1:1', mergeTreeTextIndex(currentDatabase(), tab, idx_s)); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tab_index';

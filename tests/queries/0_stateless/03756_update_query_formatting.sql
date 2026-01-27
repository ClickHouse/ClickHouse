CREATE TABLE 03756_update_query_formatting (a UInt64, b UInt64) Engine = MergeTree ORDER BY a SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
UPDATE 03756_update_query_formatting SET b = 2 WHERE (NOT a)[0] AS a0; -- { clientError SYNTAX_ERROR }
ALTER TABLE 03756_update_query_formatting UPDATE b = 2 WHERE (NOT a)[0] AS a0; -- { clientError SYNTAX_ERROR }
ALTER TABLE 03756_update_query_formatting DELETE WHERE (NOT a)[0] AS a0; -- { clientError SYNTAX_ERROR }
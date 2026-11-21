SET transform_null_in = 1;
CREATE TABLE key_analysis_context (x UInt64) ENGINE = MergeTree
ORDER BY (x IN (NULL, 1));

-- Keep the key type `UInt8` under both settings, while `in` is rewritten to `nullIn` only during metadata analysis.
-- Rebuild metadata under analysis settings different from the storage context.
ALTER TABLE key_analysis_context COMMENT COLUMN x 'Rebuild key metadata';
SET transform_null_in = 0;
INSERT INTO key_analysis_context VALUES (1), (2);
SELECT x FROM key_analysis_context ORDER BY x;
CHECK TABLE key_analysis_context SETTINGS check_query_single_value_result = 1;
DROP TABLE key_analysis_context;

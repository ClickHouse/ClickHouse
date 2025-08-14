-- Ensure that there will be at least one non-empty MergeTree table
SYSTEM FLUSH LOGS text_log;
-- NOTE: database = currentDatabase() is not mandatory
SELECT sum(data_compressed_bytes) > 0, sum(data_uncompressed_bytes) > 0, sum(marks_bytes) > 0 FROM system.columns;

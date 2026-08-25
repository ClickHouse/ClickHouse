-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage

SET allow_experimental_unique_key = 1;
SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS uk_t_stream;

CREATE TABLE uk_t_stream (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- Streaming read (FROM ... STREAM) on a unique-key table -> error.
-- The streaming source does not apply the delete-bitmap filter; reject rather
-- than serve logically-deleted rows.
-- On Linux the UK guard rejects with NOT_IMPLEMENTED; on macOS streaming is
-- disabled platform-wide (SUPPORT_IS_DISABLED) and fires first. Either way STREAM
-- on a UK table is rejected.
SELECT * FROM uk_t_stream STREAM; -- { serverError NOT_IMPLEMENTED, SUPPORT_IS_DISABLED }

DROP TABLE uk_t_stream;

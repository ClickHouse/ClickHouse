CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS marks_compress_block_size = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS primary_key_compress_block_size = 0; -- { serverError BAD_ARGUMENTS }

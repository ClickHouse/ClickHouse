DROP TABLE IF EXISTS null_;
DROP TABLE IF EXISTS buffer_;

CREATE TABLE null_ (key UInt64) Engine=Null();
CREATE TABLE buffer_ (key UInt64) Engine=Buffer(currentDatabase(), null_,
    1,    /* num_layers */
    0,    /* min_time   */
    86400,/* max_time   */
    0,    /* min_rows   */
    100e9,/* max_rows   */
    0,    /* min_bytes  */
    20e6  /* max_bytes  */
);

-- note that there is untracked_memory_limit (4MB) in MemoryTracker
SET max_memory_usage=10e6;

SET min_insert_block_size_bytes=9e6;
INSERT INTO buffer_ SELECT toUInt64(number) FROM system.numbers LIMIT 10e6; -- { serverError 241 }

OPTIMIZE TABLE buffer_; -- flush

SET min_insert_block_size_bytes=1e6;
INSERT INTO buffer_ SELECT toUInt64(number) FROM system.numbers LIMIT 10e6;

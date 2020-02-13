DROP TABLE IF EXISTS null_;
DROP TABLE IF EXISTS buffer_;

-- Each UInt64  is 8    bytes
-- So 10e6 rows is 80e6 bytes
--
-- Use LIMIT max_rows+1 to force flush from the query context, and to avoid
-- flushing from the background thread, since in this case it can steal memory
-- the max_memory_usage may be exceeded during squashing other blocks.


CREATE TABLE null_ (key UInt64) Engine=Null();
CREATE TABLE buffer_ (key UInt64) Engine=Buffer(currentDatabase(), null_,
    1,    /* num_layers */
    10e6, /* min_time, placeholder */
    10e6, /* max_time, placeholder */
    0,    /* min_rows   */
    10e6, /* max_rows   */
    0,    /* min_bytes  */
    80e6  /* max_bytes  */
);

SET max_memory_usage=10e6;
SET max_block_size=100e3;

-- Check that max_memory_usage is ignored only on flush and not on squash
SET min_insert_block_size_bytes=9e6;
SET min_insert_block_size_rows=0;
INSERT INTO buffer_ SELECT toUInt64(number) FROM system.numbers LIMIT toUInt64(10e6+1); -- { serverError 241 }

OPTIMIZE TABLE buffer_; -- flush just in case

SET min_insert_block_size_bytes=0;
SET min_insert_block_size_rows=100e3;
INSERT INTO buffer_ SELECT toUInt64(number) FROM system.numbers LIMIT toUInt64(10e6+1);
-- Check that 10e6 rows had been flushed from the query, not from the background worker.
SELECT count() FROM buffer_;

DROP TABLE null_;
DROP TABLE buffer_;

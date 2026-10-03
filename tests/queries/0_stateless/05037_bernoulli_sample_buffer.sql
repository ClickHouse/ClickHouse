-- Bernoulli `SAMPLE` must be rejected for `Buffer` tables: `StorageBuffer::read` forwards the sample
-- modifier only to the destination table, so the rows that are still in the in-memory buffers would be
-- returned unsampled next to the sampled ones.

SET allow_experimental_bernoulli_sample = 1;
SET bernoulli_sample_seed = 42;

DROP TABLE IF EXISTS t_bernoulli_buffer;
DROP TABLE IF EXISTS t_bernoulli_buffer_dst;

CREATE TABLE t_bernoulli_buffer_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_bernoulli_buffer (x UInt64) ENGINE = Buffer(currentDatabase(), t_bernoulli_buffer_dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 1000000000);

-- The rows stay in the buffer: they are neither flushed by size nor by time within the test.
INSERT INTO t_bernoulli_buffer SELECT number FROM numbers(100000);

SELECT 'all rows are still buffered';
SELECT count() FROM t_bernoulli_buffer_dst;
SELECT count() FROM t_bernoulli_buffer;

SELECT 'sample is rejected';
SELECT count() FROM t_bernoulli_buffer SAMPLE 0.1; -- { serverError SAMPLING_NOT_SUPPORTED }

SELECT 'the destination table can still be sampled directly';
SELECT count() FROM t_bernoulli_buffer_dst SAMPLE 0.1;

DROP TABLE t_bernoulli_buffer;
DROP TABLE t_bernoulli_buffer_dst;

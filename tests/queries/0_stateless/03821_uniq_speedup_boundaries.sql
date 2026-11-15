-- Basic correctness
SELECT uniq(number) FROM numbers(100);
SELECT uniq(number % 10) FROM numbers(1000);

-- Batch boundaries
SELECT uniq(number) FROM numbers(7);   -- Just below batch_size
SELECT uniq(number) FROM numbers(8);   -- Exactly batch_size
SELECT uniq(number) FROM numbers(17);  -- Two full batches + 1

-- Force resize during batching (depends on UNIQUES_HASH_MAX_SIZE)
-- Note this returns 100315 due to BJKST thinning (expect ~100000, actual varies).
-- This is expected and the same as previous releases
SELECT uniq(number) FROM numbers(100000);

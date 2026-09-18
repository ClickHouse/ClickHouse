-- Tags: no-parallel
-- Do not run in parallel: a rootless `CREATE WORKLOAD` claims the single global root-workload
-- slot enforced in `WorkloadEntityStorageBase::storeEntityImpl`, so it races with any other test
-- that creates a root workload and one side fails with `The second root is not allowed`.

-- Out-of-range ratios must not produce undefined behaviour when narrowed to `Int64`.
-- The oracle is the sanitizer: under UBSan the statements below abort without the clamp.

CREATE WORKLOAD 04718_all;

-- `max_concurrent_threads_ratio_to_cores`: `ratio * cores` exceeds `Int64` range.
CREATE WORKLOAD 04718_w1 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 9223372036854775807;
CREATE WORKLOAD 04718_w2 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 9223372036854775807.0;
CREATE WORKLOAD 04718_w3 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 1e19;
CREATE WORKLOAD 04718_w4 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 1e100;
CREATE WORKLOAD 04718_w5 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 1.7976931348623157e308;

-- In-range values must keep working unchanged.
CREATE WORKLOAD 04718_w6 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 0;
CREATE WORKLOAD 04718_w7 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 0.5;
CREATE WORKLOAD 04718_w8 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 2.5;
CREATE WORKLOAD 04718_w9 IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = 1e-9;

-- Both limits together: the resolved value is asserted in the gtest, which can read it.
CREATE WORKLOAD 04718_w10 IN 04718_all SETTINGS max_concurrent_threads = 4, max_concurrent_threads_ratio_to_cores = 1e100;

-- Already-rejected inputs must keep their existing errors.
CREATE WORKLOAD 04718_bad IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = inf; -- { serverError CANNOT_PARSE_NUMBER }
CREATE WORKLOAD 04718_bad IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = nan; -- { serverError CANNOT_PARSE_NUMBER }
CREATE WORKLOAD 04718_bad IN 04718_all SETTINGS max_concurrent_threads_ratio_to_cores = -0.1; -- { serverError BAD_ARGUMENTS }

-- Regression guard on the sibling `max_memory_ratio` clamp, which has the same shape.
CREATE WORKLOAD 04718_m1 IN 04718_all SETTINGS max_memory_ratio = 9223372036854775807;
CREATE WORKLOAD 04718_m2 IN 04718_all SETTINGS max_memory_ratio = 1e100;
CREATE WORKLOAD 04718_m3 IN 04718_all SETTINGS max_memory_ratio = 0.5;

SELECT 'OK';

DROP WORKLOAD IF EXISTS 04718_m3;
DROP WORKLOAD IF EXISTS 04718_m2;
DROP WORKLOAD IF EXISTS 04718_m1;
DROP WORKLOAD IF EXISTS 04718_w10;
DROP WORKLOAD IF EXISTS 04718_w9;
DROP WORKLOAD IF EXISTS 04718_w8;
DROP WORKLOAD IF EXISTS 04718_w7;
DROP WORKLOAD IF EXISTS 04718_w6;
DROP WORKLOAD IF EXISTS 04718_w5;
DROP WORKLOAD IF EXISTS 04718_w4;
DROP WORKLOAD IF EXISTS 04718_w3;
DROP WORKLOAD IF EXISTS 04718_w2;
DROP WORKLOAD IF EXISTS 04718_w1;
DROP WORKLOAD IF EXISTS 04718_all;

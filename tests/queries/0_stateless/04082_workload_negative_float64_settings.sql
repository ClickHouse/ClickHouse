-- Tags: no-parallel
-- Do not run in parallel: `CREATE WORKLOAD <name>` (without `IN <parent>`) claims the
-- single global root-workload slot enforced in `WorkloadEntityStorageBase::storeEntityImpl`,
-- so a rootless `CREATE WORKLOAD` races with any other test that does the same (e.g.
-- `03232_workload_create_and_drop`) and one side fails with
-- `BAD_ARGUMENTS: The second root is not allowed`. Workload names are isolated; the root
-- slot is not.
--
-- Test that negative Float64 values are rejected for workload settings.
-- Fixes https://github.com/ClickHouse/ClickHouse/issues/101825:
-- `getNotNegativeFloat64` did not validate the sign of Float64 values,
-- only Int64 values. A negative Float64 like -100.5 was silently accepted,
-- creating scheduler nodes with negative speed/burst limits.

CREATE WORKLOAD 04082_all;

-- Negative Float64 values must be rejected for all Float64 workload settings
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_bytes_per_second = -100.5; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_burst_bytes = -50.5; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_cpus = -2.5; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_cpu_share = -0.5; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_burst_cpu_seconds = -1.5; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_queries_per_second = -3.14; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_burst_queries = -7.7; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_concurrent_threads_ratio_to_cores = -0.1; -- {serverError BAD_ARGUMENTS}

-- Negative Int64 values must still be rejected (existing behavior)
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_bytes_per_second = -1; -- {serverError BAD_ARGUMENTS}
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_io_requests = -1; -- {serverError BAD_ARGUMENTS}

-- Positive Float64 values must still be accepted
CREATE WORKLOAD 04082_w IN 04082_all SETTINGS max_bytes_per_second = 100.5;
SELECT 'OK';

DROP WORKLOAD IF EXISTS 04082_w;
DROP WORKLOAD IF EXISTS 04082_all;

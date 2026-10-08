-- Tags: no-fasttest, no-msan
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100502
--
-- delta_lake_snapshot_version and CDF settings must raise an error
-- when DeltaKernel is not active (legacy metadata reader),
-- instead of silently returning wrong data.

SET allow_experimental_delta_kernel_rs = 0;

-- Time travel without kernel must fail
SELECT count()
FROM deltaLake('http://localhost:11111/test/delta_table', NOSIGN)
SETTINGS delta_lake_snapshot_version = 0; -- { serverError UNSUPPORTED_METHOD }

-- Negative non-default value must also fail
SELECT count()
FROM deltaLake('http://localhost:11111/test/delta_table', NOSIGN)
SETTINGS delta_lake_snapshot_version = -2; -- { serverError UNSUPPORTED_METHOD }

-- CDF start version without kernel must fail
SELECT count()
FROM deltaLake('http://localhost:11111/test/delta_table', NOSIGN)
SETTINGS delta_lake_snapshot_start_version = 0; -- { serverError UNSUPPORTED_METHOD }

-- CDF end version without kernel must fail
SELECT count()
FROM deltaLake('http://localhost:11111/test/delta_table', NOSIGN)
SETTINGS delta_lake_snapshot_end_version = 0; -- { serverError UNSUPPORTED_METHOD }

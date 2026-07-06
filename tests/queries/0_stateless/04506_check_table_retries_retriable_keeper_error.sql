-- Tags: replica, no-parallel
-- no-parallel: the fault injection failpoint is a global server switch.

DROP TABLE IF EXISTS check_keeper_retry SYNC;

CREATE TABLE check_keeper_retry (a UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/04506_check_table_retries', 'r1')
ORDER BY tuple();

INSERT INTO check_keeper_retry VALUES (1);

-- Sanity: the healthy table passes without fault injection.
CHECK TABLE check_keeper_retry SETTINGS check_query_single_value_result = 1;

-- A single retriable Keeper hardware error must NOT be reported as a broken part.
-- The failpoint fires once, the retry then succeeds, so CHECK still returns 1.
SYSTEM ENABLE FAILPOINT check_data_part_zk_hardware_error;
CHECK TABLE check_keeper_retry SETTINGS check_query_single_value_result = 1, keeper_max_retries = 10, keeper_retry_initial_backoff_ms = 1, keeper_retry_max_backoff_ms = 10;
SYSTEM DISABLE FAILPOINT check_data_part_zk_hardware_error;

-- With retries disabled the retriable error is surfaced as a query error instead of a spurious 0.
SYSTEM ENABLE FAILPOINT check_data_part_zk_hardware_error;
CHECK TABLE check_keeper_retry SETTINGS check_query_single_value_result = 1, keeper_max_retries = 0; -- { serverError KEEPER_EXCEPTION }
SYSTEM DISABLE FAILPOINT check_data_part_zk_hardware_error;

DROP TABLE check_keeper_retry SYNC;

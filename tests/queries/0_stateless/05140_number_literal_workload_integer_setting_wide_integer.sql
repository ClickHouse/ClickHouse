-- Tags: no-parallel
-- Do not run in parallel: `CREATE WORKLOAD <name>` without `IN <parent>` claims the single global
-- root-workload slot, so it races with any other test that creates a rootless workload.

-- An integral workload setting accepts any number literal. A literal too large for UInt64 resolves
-- to a wide integer and is saturated to the maximum, and a literal such as `1e3` is a number too.
-- Reading either as Int64 without converting fails with `Bad get`.

CREATE OR REPLACE WORKLOAD all SETTINGS max_io_requests = 18446744073709551616, max_concurrent_queries = 1e3;
SELECT create_query FROM system.workloads WHERE name = 'all';

-- A fractional value is still not an integer.
CREATE WORKLOAD invalid IN all SETTINGS max_io_requests = 1.5; -- { serverError BAD_GET }

DROP WORKLOAD all;

-- Tags: no-parallel
-- Do not run in parallel: `CREATE WORKLOAD <name>` without `IN <parent>` claims the single global
-- root-workload slot, so it races with any other test that creates a rootless workload.

-- A workload setting is a Float64 too, and a literal too large for UInt64 resolves to a wide
-- integer. Reading it as Float64 without converting fails with `Bad get`.

CREATE OR REPLACE WORKLOAD all SETTINGS max_bytes_per_second = 18446744073709551616;
SELECT create_query FROM system.workloads WHERE name = 'all';
DROP WORKLOAD all;

-- Tags: no-parallel
-- Tag no-parallel: creates, replaces, and drops the process-global root `WORKLOAD all`, which
-- would disrupt query scheduling for any concurrently running test
CREATE OR REPLACE WORKLOAD all;
SELECT name, parent, create_query FROM system.workloads ORDER BY name;
CREATE WORKLOAD IF NOT EXISTS production IN all;
CREATE WORKLOAD development IN all;
SELECT name, parent, create_query FROM system.workloads ORDER BY name;
DROP WORKLOAD IF EXISTS production;
DROP WORKLOAD development;
SELECT name, parent, create_query FROM system.workloads ORDER BY name;
DROP WORKLOAD all;

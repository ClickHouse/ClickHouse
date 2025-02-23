-- Test for COW Snapshot Query
SELECT * FROM hudi_table_cow_snapshot WHERE column = 'test';

-- Test for COW Incremental Query
SELECT * FROM hudi_table_cow_incremental WHERE column > 100;

-- Test for COW CDC Query
SELECT * FROM hudi_table_cow_cdc WHERE timestamp > '2024-01-01 00:00:00';

-- Test for COW Bootstrap Query
SELECT * FROM hudi_table_cow_bootstrap WHERE column IS NOT NULL;

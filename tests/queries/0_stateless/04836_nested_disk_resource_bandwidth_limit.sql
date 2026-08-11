-- Tags: no-parallel
-- Do not run in parallel: `CREATE WORKLOAD` without `IN <parent>` claims the single
-- global root-workload slot, so a rootless `CREATE WORKLOAD` races with any other
-- test that does the same.

-- Test for nested disk independent RESOURCE bandwidth limiting.
-- Verifies that two RESOURCEs bound to different disk names (simulating
-- cached_oss -> oss nesting) can have independent throttler rates via
-- the `FOR <resource>` clause in CREATE WORKLOAD.
--
-- The inner resource link resolution added in IOSchedulingSettings.cpp
-- (updateIOSchedulingSettingsImpl) enables CachedObjectStorage to swap
-- ResourceLink at delegation points so the inner disk's RESOURCE throttler
-- applies to S3 I/O incurred by cache misses.

-- ========================================================================
-- Stage 1: Create RESOURCEs for outer (cache) and inner (object storage) disks
-- ========================================================================
CREATE RESOURCE 04836_io_cached (READ DISK 04836_cached_disk, WRITE DISK 04836_cached_disk);
CREATE RESOURCE 04836_io_oss (READ DISK 04836_oss_disk, WRITE DISK 04836_oss_disk);

-- Verify resources are created with correct disk bindings
SELECT name, read_disks, write_disks FROM system.resources
WHERE name LIKE '04836_%' ORDER BY name;

-- ========================================================================
-- Stage 2: Create WORKLOAD with different bandwidth limits per resource
-- ========================================================================
CREATE WORKLOAD 04836_all SETTINGS
    max_bytes_per_second FOR 04836_io_cached = 200000000,
    max_bytes_per_second FOR 04836_io_oss = 50000000;

-- Verify workload creation and FOR clause is captured in create_query
SELECT name, parent, create_query FROM system.workloads WHERE name = '04836_all';

-- Verify each resource has a throttler node with the correct max_speed
SELECT resource, type, CAST(max_speed, 'Int64') AS max_speed FROM system.scheduler
WHERE type = 'bandwidth_limit' AND resource LIKE '04836_%'
ORDER BY resource;

-- ========================================================================
-- Stage 3: CREATE OR REPLACE to update throttler for only one resource
-- ========================================================================
CREATE OR REPLACE WORKLOAD 04836_all SETTINGS
    max_bytes_per_second FOR 04836_io_cached = 100000000,
    max_burst_bytes FOR 04836_io_cached = 200000000;

-- Verify that io_cached throttler is updated, io_oss throttler is removed
SELECT resource, type, CAST(max_speed, 'Int64') AS max_speed FROM system.scheduler
WHERE type = 'bandwidth_limit' AND resource LIKE '04836_%'
ORDER BY resource;

-- ========================================================================
-- Stage 4: Error cases
-- ========================================================================

-- Duplicate resource name should fail
CREATE RESOURCE 04836_io_cached (READ DISK 04836_another_disk); -- {serverError BAD_ARGUMENTS}

-- FOR clause with non-existent resource should fail
CREATE WORKLOAD 04836_invalid IN 04836_all SETTINGS
    max_bytes_per_second FOR nonexistent_resource = 1000000; -- {serverError BAD_ARGUMENTS}

-- ========================================================================
-- Cleanup
-- ========================================================================
DROP WORKLOAD IF EXISTS 04836_invalid;
DROP WORKLOAD IF EXISTS 04836_all;
DROP RESOURCE IF EXISTS 04836_io_cached;
DROP RESOURCE IF EXISTS 04836_io_oss;

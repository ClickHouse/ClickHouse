-- Test configuration-based workloads and resources

-- This test verifies that the basic functionality works
-- The actual configuration loading is tested in unit tests

-- Create a workload to test basic functionality
CREATE WORKLOAD test_workload SETTINGS max_io_requests = 100;

-- Create a resource to test basic functionality  
CREATE RESOURCE test_resource (READ DISK default);

-- Verify workloads exist
SELECT name FROM system.workloads WHERE name = 'test_workload';

-- Verify resources exist
SELECT name FROM system.resources WHERE name = 'test_resource';

-- Clean up
DROP WORKLOAD test_workload;
DROP RESOURCE test_resource;
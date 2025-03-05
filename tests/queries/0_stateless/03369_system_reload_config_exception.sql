-- Test that SYSTEM RELOAD CONFIG properly creates exceptions
SET send_logs_level = 'none';

-- Test local mode (should throw exception)
CREATE DATABASE IF NOT EXISTS test_02500;
USE test_02500;

SELECT 'Test 1: Check exception in local mode';
SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }

DROP DATABASE IF EXISTS test_02500; 

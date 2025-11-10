-- Test that the arrow_flight_request_descriptor_type setting exists and accepts valid values

-- Test default value
SELECT name, value, changed, description FROM system.settings WHERE name = 'arrow_flight_request_descriptor_type';

-- Test setting to 'path'
SET arrow_flight_request_descriptor_type = 'path';
SELECT value FROM system.settings WHERE name = 'arrow_flight_request_descriptor_type';

-- Test setting to 'command'
SET arrow_flight_request_descriptor_type = 'command';
SELECT value FROM system.settings WHERE name = 'arrow_flight_request_descriptor_type';

-- Test that invalid value throws error (should fail)
SET arrow_flight_request_descriptor_type = 'invalid'; -- { serverError BAD_ARGUMENTS }

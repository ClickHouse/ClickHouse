CREATE TABLE t_custom_disk_error (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS disk = disk(type = 'unknown_disk_type', secret_access_key = 'P@ssw0rd'); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

CREATE TABLE t_custom_disk_error (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS disk = disk(type = 'cache', max_size = '1Mi', path = 'custom_disk_error_hides_secrets/', disk = disk(type = 'unknown_disk_type', secret_access_key = 'P@ssw0rd')); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

SELECT 1;

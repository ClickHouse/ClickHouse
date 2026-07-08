-- Setting a query parameter whose name collides with a builtin setting
-- (limit / offset / max_threads / log_comment) used to poison the client
-- parameter map so every later SET param_* failed with CANNOT_PARSE_QUOTED_STRING.

SET param_limit = 2;
SET param_other = 5;
SELECT {other:UInt32};

SELECT number FROM numbers(10) LIMIT {limit:UInt8};

SET param_offset = 7;
SELECT {offset:UInt32};

SET param_log_comment = 'hi';
SET param_z = 3;
SELECT {z:UInt8}, {log_comment:String};

-- Non-colliding parameters keep working and string values are preserved.
SET param_name = 'John Doe';
SELECT {name:String};

-- A parameter whose name collides with a setting that normalizes its value must
-- round-trip the original text, not the setting's normalized form. max_threads = 0
-- is stored as auto(<cores>) by SettingFieldMaxThreads, which used to leak into the
-- parameter value (issue #85768).
SET param_max_threads = 0;
SELECT {max_threads:UInt64};

-- A parameter whose name collides with a setting ALIAS must not be alias-resolved
-- into the canonical setting name and value. insert_distributed_sync is an alias of
-- distributed_foreground_insert (a Bool), which used to reject / rewrite the value.
SET param_insert_distributed_sync = 5;
SELECT {insert_distributed_sync:UInt8};

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

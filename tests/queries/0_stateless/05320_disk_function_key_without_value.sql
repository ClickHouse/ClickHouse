-- A key without a value or another malformed argument in `disk(...)` is rejected cleanly, and the query log keeps every secret value hidden.

SET log_queries = 1, log_queries_min_type = 'QUERY_START', log_queries_probability = 1;

SET max_threads = disk(equals(password)); -- { serverError CANNOT_CONVERT_TYPE }
SET max_threads = disk(equals(password, 'disk_function_first_value_canary', 'disk_function_extra_value_canary')); -- { serverError CANNOT_CONVERT_TYPE }

-- A malformed `disk(...)` must not stop the masking of a well-formed one in the same query.
SET max_threads = disk(type = 'local', 'disk_function_malformed_canary'), max_block_size = disk(type = 'local', path = 'disk_function_malformed_canary/'); -- { serverError CANNOT_CONVERT_TYPE }
SET max_threads = disk(equals()), max_block_size = disk(type = 'local', path = 'disk_function_malformed_canary/'); -- { serverError CANNOT_CONVERT_TYPE }
SET max_threads = disk(equals('password', 'disk_function_malformed_canary')), max_block_size = disk(type = 'local', path = 'disk_function_malformed_canary/'); -- { serverError CANNOT_CONVERT_TYPE }

CREATE TABLE t_secret_key (a Int) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = 'local', path = 'disk_function_key_without_value_canary/', equals(password)); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_plain_key (a Int) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = 'local', path = 'disk_function_key_without_value/', equals(name)); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_nested (a Int) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = 'cache', max_size = '1Mi', path = 'disk_function_key_without_value_cache/', disk = disk(equals(password))); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_positional (a Int) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = 'local', path = 'disk_function_malformed_canary/', 1); -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

-- The statements carrying a canary are logged with their values hidden, and no canary is logged.
SELECT
    countIf(position(query, 't_secret' || '_key') > 0 AND position(query, '[HIDDEN]') > 0) > 0,
    countIf(position(query, 'key_without_value' || '_canary') > 0 OR position(query, 'first_value' || '_canary') > 0
        OR position(query, 'extra_value' || '_canary') > 0 OR position(query, 'malformed' || '_canary') > 0),
    countIf(position(query, 'disk(equals(pass' || 'word, ') > 0 AND position(query, '[HIDDEN]') > 0) > 0,
    countIf(position(query, 'max_block' || '_size') > 0 AND position(query, '[HIDDEN]') > 0) > 0,
    countIf(position(query, 't_posit' || 'ional') > 0 AND position(query, '[HIDDEN]') > 0) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday();

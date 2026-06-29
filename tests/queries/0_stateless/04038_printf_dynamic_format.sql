-- Verify that printf supports dynamic (non-constant) format strings.
-- See https://github.com/ClickHouse/ClickHouse/issues/89547

-- Test 1: Constant format string still works
SELECT printf('%s: %d', 'count', 42);

-- Test 2: Dynamic format string from column
SELECT printf(fmt, 'hello', 123) FROM (SELECT arrayJoin(['%s=%d', '[%s] %d']) AS fmt);

-- Test 3: Dynamic format from table
DROP TABLE IF EXISTS test_printf_fmt;
CREATE TABLE test_printf_fmt (fmt String, val UInt32) ENGINE = Memory;
INSERT INTO test_printf_fmt VALUES ('%d items', 5), ('total: %d', 10), ('count=%d', 0);
SELECT printf(fmt, val) FROM test_printf_fmt;
DROP TABLE test_printf_fmt;

-- Test 4: Dynamic format with materialized column
SELECT printf(materialize('%s world'), 'hello');

-- Test 5: Empty format string — throws because format accounts for one argument slot
SELECT printf(materialize('')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Test 6: Format with only %% escapes
SELECT printf(materialize('100%%'));

-- Test 7: Dynamic format — argument count mismatch (too few args)
SELECT printf(materialize('%s %d'), 'hello'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Test 8: Dynamic format — argument count mismatch (too many args)
SELECT printf(materialize('%s'), 'hello', 123); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

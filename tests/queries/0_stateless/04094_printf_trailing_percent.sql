-- Test printf with trailing '%' and various %% escaping edge cases.
-- Covers both constant and dynamic (materialized) format paths.
-- Regression test for MSan read-past-end in fmt::sprintf with lone '%'.

-- Constant format: lone '%' with argument (treated as literal)
SELECT printf('%', 42); -- { serverError BAD_ARGUMENTS }
SELECT printf('%', 'hello'); -- { serverError BAD_ARGUMENTS }

-- Materialized format: lone '%' with argument
SELECT printf(materialize('%'), 42); -- { serverError BAD_ARGUMENTS }
SELECT printf(materialize('%'), 'hello'); -- { serverError BAD_ARGUMENTS }

-- Constant format: lone '%' without argument (error)
SELECT printf('%'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Materialized format: lone '%' without argument (error)
SELECT printf(materialize('%')); -- { serverError BAD_ARGUMENTS }

-- Trailing '%' after literal text
SELECT printf('abc%', 42); -- { serverError BAD_ARGUMENTS }
SELECT printf(materialize('abc%'), 42); -- { serverError BAD_ARGUMENTS }

-- Double percent escaping
SELECT printf('%%');
SELECT printf(materialize('%%'));
SELECT printf('hello %%');
SELECT printf(materialize('hello %%'));
SELECT printf('%%val');
SELECT printf(materialize('%%val'));
SELECT printf('a%%b%%c');
SELECT printf(materialize('a%%b%%c'));

-- Double percent with arguments
SELECT printf('%% %d', 42);
SELECT printf(materialize('%% %d'), 42);
SELECT printf('%d %%', 42);
SELECT printf(materialize('%d %%'), 42);
SELECT printf('%d %% %s', 42, 'hi');
SELECT printf(materialize('%d %% %s'), 42, 'hi');

-- Multiple trailing percents
SELECT printf('%%%%');
SELECT printf(materialize('%%%%'));

-- Normal format specifiers (sanity check)
SELECT printf('%d', 42);
SELECT printf(materialize('%d'), 42);
SELECT printf('%s', 'hello');
SELECT printf(materialize('%s'), 'hello');
SELECT printf('val=%d end', 100);
SELECT printf(materialize('val=%d end'), 100);
SELECT printf('%d %s %d', 1, 'x', 2);
SELECT printf(materialize('%d %s %d'), 1, 'x', 2);

-- Dynamic format from column
SELECT printf(s, 42) FROM (SELECT arrayJoin(['%d', '%5d', '%x', '%o']) AS s);
SELECT printf(s) FROM (SELECT arrayJoin(['hello', '%%', 'abc%%def', '%%%%']) AS s);

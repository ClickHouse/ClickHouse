-- Test that the 'error' hint works for syntax/parsing errors (client-side).
-- The 'error' hint sets both server_errors and client_errors, so the
-- PARSING_EXCEPTION handler must fall through to the client error check
-- instead of rejecting with "Expected server error".
-- See: https://github.com/ClickHouse/ClickHouse/issues/101664

-- Basic: 'error' hint on a syntax error should pass
SELEC 1; -- { error SYNTAX_ERROR }

-- Verify the session continues normally after the handled error
SELECT 'after_error_hint';

-- Multiple syntax errors with 'error' hint
SELEC; -- { error SYNTAX_ERROR }
FROMM system.one; -- { error SYNTAX_ERROR }

-- 'clientError' still works for syntax errors (existing behavior)
SELEC 1; -- { clientError SYNTAX_ERROR }

-- Verify the session is still healthy
SELECT 'all_passed';

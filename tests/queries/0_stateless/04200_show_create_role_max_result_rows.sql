-- Tags: no-parallel

DROP ROLE IF EXISTS role_04200_a, role_04200_b;
CREATE ROLE role_04200_a;
CREATE ROLE role_04200_b;

SET max_result_rows = 1, result_overflow_mode = 'throw';
SHOW CREATE ROLE role_04200_a, role_04200_b; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SHOW CREATE ROLE role_04200_a;

SET max_result_rows = 0;
SHOW CREATE ROLE role_04200_a, role_04200_b;

DROP ROLE role_04200_a, role_04200_b;

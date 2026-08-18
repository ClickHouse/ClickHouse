-- The `sort` construction setting takes comma-separated column names and 1-based positional
-- references (with an optional `+`/`-` direction prefix). Malformed elements must fail with a
-- clear BAD_ARGUMENTS error instead of silently producing wrong or no-op ordering:
--   - a zero positional reference (`sort=0`) would otherwise become a constant `ORDER BY 0` no-op;
--   - an empty element from a stray/leading/trailing comma (`sort=,a` / `sort=a,`) would otherwise
--     be silently dropped.

SELECT '-- valid: 1-based positional reference (position 1 is the boundary that must be accepted)';
SELECT number FROM numbers(3) SETTINGS sort = '1';
SELECT '-- valid: descending positional reference';
SELECT number FROM numbers(3) SETTINGS sort = '-1';
SELECT '-- valid: a normal comma-separated list is still accepted';
SELECT number FROM numbers(3) SETTINGS sort = 'number,1';

SELECT '-- invalid: a zero positional reference is rejected, not treated as ORDER BY 0';
SELECT number FROM numbers(3) SETTINGS sort = '0'; -- { serverError BAD_ARGUMENTS }
SELECT number FROM numbers(3) SETTINGS sort = '-0'; -- { serverError BAD_ARGUMENTS }
SELECT number FROM numbers(3) SETTINGS sort = '00'; -- { serverError BAD_ARGUMENTS }

SELECT '-- invalid: an empty element (stray/leading/trailing comma) is rejected, not silently dropped';
SELECT number FROM numbers(3) SETTINGS sort = ',number'; -- { serverError BAD_ARGUMENTS }
SELECT number FROM numbers(3) SETTINGS sort = 'number,'; -- { serverError BAD_ARGUMENTS }
SELECT number FROM numbers(3) SETTINGS sort = 'number,,number'; -- { serverError BAD_ARGUMENTS }

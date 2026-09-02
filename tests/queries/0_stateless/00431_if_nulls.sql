/* Condition could be:
 * - constant, true
 * - constant, false
 * - constant, NULL
 * - non constant, non nullable_00431
 * - non constant, nullable_00431
 *
 * Then and else could be:
 * - constant, not NULL
 * - constant, NULL
 * - non constant, non nullable_00431
 * - non constant, nullable_00431
 *
 * Thus we have 5 * 4 * 4 = 80 combinations.
 */

DROP TABLE IF EXISTS nullable_00431;

CREATE VIEW nullable_00431
AS SELECT
    1 AS constant_true,
    0 AS constant_false,
    NULL AS constant_null,
    number % 3 = 1 AS cond_non_constant,
    number % 3 = 2 ? NULL : (number % 3 = 1) AS cond_non_constant_nullable,
    'Hello' AS then_constant,
    'World' AS else_constant,
    toString(number) AS then_non_constant,
    toString(-number) AS else_non_constant,
    nullIf(toString(number), '5') AS then_non_constant_nullable,
    nullIf(toString(-number), '-5') AS else_non_constant_nullable
FROM system.numbers LIMIT 10;

SELECT '---------- constant_true ----------';

SELECT constant_true ? then_constant : else_constant AS res FROM nullable_00431;
SELECT constant_true ? then_constant : constant_null AS res FROM nullable_00431;
SELECT constant_true ? then_constant : else_non_constant AS res FROM nullable_00431;
SELECT constant_true ? then_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_true ? constant_null : else_constant AS res FROM nullable_00431;
SELECT constant_true ? constant_null : constant_null AS res FROM nullable_00431;
SELECT constant_true ? constant_null : else_non_constant AS res FROM nullable_00431;
SELECT constant_true ? constant_null : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_true ? then_non_constant : else_constant AS res FROM nullable_00431;
SELECT constant_true ? then_non_constant : constant_null AS res FROM nullable_00431;
SELECT constant_true ? then_non_constant : else_non_constant AS res FROM nullable_00431;
SELECT constant_true ? then_non_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_true ? then_non_constant_nullable : else_constant AS res FROM nullable_00431;
SELECT constant_true ? then_non_constant_nullable : constant_null AS res FROM nullable_00431;
SELECT constant_true ? then_non_constant_nullable : else_non_constant AS res FROM nullable_00431;
SELECT constant_true ? then_non_constant_nullable : else_non_constant_nullable AS res FROM nullable_00431;

SELECT '---------- constant_false ----------';

SELECT constant_false ? then_constant : else_constant AS res FROM nullable_00431;
SELECT constant_false ? then_constant : constant_null AS res FROM nullable_00431;
SELECT constant_false ? then_constant : else_non_constant AS res FROM nullable_00431;
SELECT constant_false ? then_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_false ? constant_null : else_constant AS res FROM nullable_00431;
SELECT constant_false ? constant_null : constant_null AS res FROM nullable_00431;
SELECT constant_false ? constant_null : else_non_constant AS res FROM nullable_00431;
SELECT constant_false ? constant_null : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_false ? then_non_constant : else_constant AS res FROM nullable_00431;
SELECT constant_false ? then_non_constant : constant_null AS res FROM nullable_00431;
SELECT constant_false ? then_non_constant : else_non_constant AS res FROM nullable_00431;
SELECT constant_false ? then_non_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_false ? then_non_constant_nullable : else_constant AS res FROM nullable_00431;
SELECT constant_false ? then_non_constant_nullable : constant_null AS res FROM nullable_00431;
SELECT constant_false ? then_non_constant_nullable : else_non_constant AS res FROM nullable_00431;
SELECT constant_false ? then_non_constant_nullable : else_non_constant_nullable AS res FROM nullable_00431;

SELECT '---------- constant_null ----------';

SELECT constant_null ? then_constant : else_constant AS res FROM nullable_00431;
SELECT constant_null ? then_constant : constant_null AS res FROM nullable_00431;
SELECT constant_null ? then_constant : else_non_constant AS res FROM nullable_00431;
SELECT constant_null ? then_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_null ? constant_null : else_constant AS res FROM nullable_00431;
SELECT constant_null ? constant_null : constant_null AS res FROM nullable_00431;
SELECT constant_null ? constant_null : else_non_constant AS res FROM nullable_00431;
SELECT constant_null ? constant_null : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_null ? then_non_constant : else_constant AS res FROM nullable_00431;
SELECT constant_null ? then_non_constant : constant_null AS res FROM nullable_00431;
SELECT constant_null ? then_non_constant : else_non_constant AS res FROM nullable_00431;
SELECT constant_null ? then_non_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT constant_null ? then_non_constant_nullable : else_constant AS res FROM nullable_00431;
SELECT constant_null ? then_non_constant_nullable : constant_null AS res FROM nullable_00431;
SELECT constant_null ? then_non_constant_nullable : else_non_constant AS res FROM nullable_00431;
SELECT constant_null ? then_non_constant_nullable : else_non_constant_nullable AS res FROM nullable_00431;

SELECT '---------- cond_non_constant ----------';

SELECT cond_non_constant ? then_constant : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? then_constant : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant ? then_constant : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? then_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT cond_non_constant ? constant_null : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? constant_null : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant ? constant_null : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? constant_null : else_non_constant_nullable AS res FROM nullable_00431;

SELECT cond_non_constant ? then_non_constant : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? then_non_constant : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant ? then_non_constant : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? then_non_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT cond_non_constant ? then_non_constant_nullable : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? then_non_constant_nullable : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant ? then_non_constant_nullable : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant ? then_non_constant_nullable : else_non_constant_nullable AS res FROM nullable_00431;

SELECT '---------- cond_non_constant_nullable ----------';

SELECT cond_non_constant_nullable ? then_constant : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_constant : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_constant : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT cond_non_constant_nullable ? constant_null : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? constant_null : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? constant_null : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? constant_null : else_non_constant_nullable AS res FROM nullable_00431;

SELECT cond_non_constant_nullable ? then_non_constant : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_non_constant : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_non_constant : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_non_constant : else_non_constant_nullable AS res FROM nullable_00431;

SELECT cond_non_constant_nullable ? then_non_constant_nullable : else_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_non_constant_nullable : constant_null AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_non_constant_nullable : else_non_constant AS res FROM nullable_00431;
SELECT cond_non_constant_nullable ? then_non_constant_nullable : else_non_constant_nullable AS res FROM nullable_00431;


DROP TABLE nullable_00431;

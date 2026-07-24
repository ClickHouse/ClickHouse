-- This functions should not be called directly, only for internal use.
-- However, we cannot completely forbid it (becasue query can came from another server, for example)
-- Check that usage of these functions does not lead to crash or logical error

SELECT __actionName(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __actionName('aaa', 'aaa', 'aaa'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __actionName('aaa', '') SETTINGS enable_analyzer = 1; -- { serverError BAD_ARGUMENTS }
SELECT __actionName('aaa', materialize('aaa')); -- { serverError BAD_ARGUMENTS,ILLEGAL_COLUMN }
SELECT __actionName(materialize('aaa'), 'aaa'); -- { serverError ILLEGAL_COLUMN }
SELECT __actionName('aaa', 'aaa');

SELECT concat(__actionName('aaa', toNullable('x')), '1') GROUP BY __actionName('aaa', 'x'); -- { serverError BAD_ARGUMENTS }

SELECT __getScalar('aaa'); -- { serverError BAD_ARGUMENTS }
SELECT __getScalar(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __getScalar(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __getScalar(materialize('1')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT __scalarSubqueryResult('1');
SELECT 'a' || __scalarSubqueryResult(a), materialize('1') as a;
SELECT __scalarSubqueryResult(a, a), materialize('1') as a; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 1 as `__grouping_set`;

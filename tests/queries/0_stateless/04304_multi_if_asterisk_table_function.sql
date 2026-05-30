-- Tags: no-random-settings

-- Regression test for the LOGICAL_ERROR "Trying to get name of not a column: ExpressionList"
-- (STID 2310-3791, related to issues #43199, #97988, #100323) that aborted debug builds.
--
-- Root cause: `resolveFunction`'s `multiIf` constant-folding helper held its condition argument
-- through a reference (`auto & cond_node = multi_if_args[2 * pair];`). When the condition was a
-- matcher (`*`), `resolveMatcher` rebinds the slot in-place to an empty `ListNode`. If a later
-- argument then threw `UNKNOWN_IDENTIFIER`, the catch in `resolveTableFunction` left a corrupted
-- argument tree whose `toAST` emits an `ASTExpressionList` where the matcher used to be;
-- `IAST::getColumnName` then raised the `LOGICAL_ERROR` and aborted in debug.
--
-- Fix: snapshot the condition into a local copy before calling `resolveExpressionNode`
-- (matches the `is_special_function_if` pattern). The matcher rebinding now hits the local
-- copy; the slot in `multi_if_args` keeps the original matcher, and the generic argument
-- resolution path takes over without producing a corrupt tree.

-- Direct repro from the fuzzer (`numbers(multiIf(*, ...), 2)` with an unresolved identifier
-- inside the value branch). Must report a normal `UNSUPPORTED_METHOD` for the unresolvable `*`,
-- not a `LOGICAL_ERROR`.
SELECT if(number % 2, materialize(toLowCardinality('a')), materialize(toLowCardinality('a'))) FROM numbers(multiIf(*, moduloOrZero(number, NULL), 'a'), 2); -- { serverError UNSUPPORTED_METHOD }

-- Same shape with a bare unresolved identifier as the value branch.
SELECT * FROM numbers(multiIf(*, number, 1), 2); -- { serverError UNSUPPORTED_METHOD }

-- Same shape without any failing branch: the asterisk alone should still be rejected.
SELECT * FROM numbers(multiIf(*, 1, 2), 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A multiIf whose condition is a real expression must keep working: the fix only changes how
-- the condition slot is held, not the constant-folding behaviour.
SELECT count() FROM numbers(multiIf(1, 5, 10), 1);
SELECT count() FROM numbers(multiIf(0, 5, 10), 1);

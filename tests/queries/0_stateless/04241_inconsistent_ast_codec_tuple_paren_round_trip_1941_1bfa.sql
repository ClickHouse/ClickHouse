-- STID 1941-1bfa: queries like CREATE TABLE t (c0 Int CODEC(not((not(materialize(1),
-- materialize(2)))), ZSTD)) ENGINE = Memory aborted the server with
-- `Inconsistent AST formatting` LOGICAL_ERROR in debug / sanitizer builds.
--
-- Root cause: inside `CODEC` / `STATISTICS` / `BACKUP_NAME` argument lists
-- `FormatStateStacked::allow_operators` is forced to `false`, so a multi-argument
-- `Function_tuple` falls back to the function-call form `tuple(arg, arg, ...)`
-- instead of the operator form `(arg, arg, ...)`. The parser then sees that
-- `tuple(...)` wrapped in `(...)` (from the outer `not(...)` parenthesised
-- argument) and `RoundBracketsLayer::getResultImpl` sets `parenthesized = true`
-- on the unwrapped `Function_tuple`. On the next format pass the `parenthesized`
-- flag emits an extra `(...)` around `tuple(...)`, so the format-parse-format
-- round-trip diverges by one paren level (STID 1941-1bfa).
--
-- The fix suppresses the `parenthesized` flag's parens for multi-argument
-- `Function_tuple` when `frame.allow_operators` is `false`, mirroring the
-- existing literal-tuple suppression in `decideParensEmission`. Both
-- `tuple(a, b)` and `(tuple(a, b))` denote the same value, so dropping the
-- redundant outer paren in this specific formatting context is semantics-
-- preserving and keeps the round-trip stable.

-- Original reproducer. We do not care which error fires, only that the server
-- does not abort with LOGICAL_ERROR. `not` is not a valid codec family, so the
-- parser returns UNKNOWN_CODEC once the round-trip check passes cleanly.
CREATE TABLE t_1941_1bfa (c0 Int CODEC(not((not(materialize(1), materialize(2)))), ZSTD)) ENGINE = Memory; -- { serverError UNKNOWN_CODEC }

-- Round-trip through `formatQuerySingleLine` must succeed (this is what the
-- internal AST round-trip check does, just from inside SQL).
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(not((not(materialize(1), materialize(2)))), ZSTD)) ENGINE = Memory');

-- Variants of the same `Function_tuple`-in-function-call-form-inside-`CODEC`
-- bug. Each must round-trip cleanly. The single-argument `tuple(x)` shape is
-- unaffected by the fix (the fix only suppresses parens for multi-argument
-- tuples), so the regression test covers both single- and multi-argument
-- forms to assert the fix did not over-reach.
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(not((tuple(1, 2))), ZSTD)) ENGINE = Memory');
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(not(tuple(1, 2, 3)), ZSTD)) ENGINE = Memory');
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(not((tuple(materialize(1), materialize(2), materialize(3)))), ZSTD)) ENGINE = Memory');

-- Single-argument `tuple(x)` keeps its `parenthesized` flag behavior — the
-- fix is gated on `args.size() > 1`.
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int CODEC(not(tuple(1)), ZSTD)) ENGINE = Memory');

-- Same shape under `STATISTICS` (also sets `allow_operators` = false).
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY STATISTICS c0 TYPE concatAssumeInjective((1, 2))');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY STATISTICS c0 TYPE concatAssumeInjective(tuple(1, 2))');

-- Real `CODEC` declarations must keep their original formatting; the fix
-- only touches the `parenthesized` flag and leaves the function-call form
-- as-is.
DROP TABLE IF EXISTS t_codec_real;
CREATE TABLE t_codec_real (c0 Int CODEC(Delta(8), LZ4)) ENGINE = Memory;
SHOW CREATE TABLE t_codec_real;
DROP TABLE t_codec_real;

-- Plain tuple usage outside `CODEC` / `STATISTICS` is unaffected — the fix
-- is gated on `frame.allow_operators == false`. The operator form `(a, b)`
-- is the canonical form in expression context and must round-trip to itself.
SELECT formatQuerySingleLine('SELECT tuple(1, 2), (1, 2), (tuple(1, 2))');
SELECT formatQuerySingleLine('SELECT 1 WHERE (1, 2) NOT IN ((3, 4))');

-- Same shape under `BACKUP_NAME` (the third kind that sets `allow_operators` =
-- false at `ASTFunction.cpp:298`). Issue #105396 reported the same STID
-- 1941-1bfa firing on `RESTORE TABLE ... FROM Memory(...)` where the
-- `Memory(...)` engine name is parsed via `parseBackupName` and tagged as
-- `Kind::BACKUP_NAME`, so descendants inherit `allow_operators = false` and
-- hit the same `Function_tuple` round-trip path as `CODEC` / `STATISTICS`.
SELECT formatQuerySingleLine('RESTORE TABLE src AS dst FROM Memory(not((not(materialize(1), materialize(2)))))');
SELECT formatQuerySingleLine('BACKUP TABLE src TO Memory(not(tuple(1, 2, 3)))');
SELECT formatQuerySingleLine('BACKUP TABLE src TO Memory(not((tuple(materialize(1), materialize(2)))))');

-- And the original `RESTORE TABLE` query path must also pass the internal
-- round-trip check (this is the exact failure shape from issue #105396).
-- We do not care which error fires, only that the server does not abort
-- with LOGICAL_ERROR.
RESTORE TABLE src_1941_1bfa AS dst_1941_1bfa FROM Memory(not(materialize(1), materialize(2))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

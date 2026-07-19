-- A data type name that starts with a digit is only reachable via quoting (e.g. `8`).
-- The formatter prints type names unquoted, so `8` would round-trip into a numeric literal
-- instead of a type name, breaking the AST formatting consistency check
-- ("Inconsistent AST formatting", STID 1941-26fa). Such names must be rejected at parse time.

-- Quoted numeric type name is rejected.
SELECT formatQuery('CREATE TABLE t (`a` `8`) ENGINE = Memory'); -- { serverError SYNTAX_ERROR }

-- The original fuzzer reproducer: a numeric name nested inside a data type argument.
SELECT formatQuery('CREATE TABLE t (`a` Nullable(multiply(Decimal(76, 12), `8`))) ENGINE = MergeTree ORDER BY a'); -- { serverError SYNTAX_ERROR }

-- Numeric arguments to a data type are still accepted (the type name is a valid identifier).
SELECT formatQuerySingleLine('CREATE TABLE t (`c` Decimal(76, 12)) ENGINE = Memory');
SELECT formatQuerySingleLine('CREATE TABLE t (`c` FixedString(8)) ENGINE = Memory');
SELECT formatQuerySingleLine('CREATE TABLE t (`c` DateTime64(3)) ENGINE = Memory');
SELECT formatQuerySingleLine('CREATE TABLE t (`c` Array(Decimal(10, 2))) ENGINE = Memory');

-- A quoted type name that is a valid identifier still round-trips (backticks dropped, but stable).
SELECT formatQuerySingleLine('CREATE TABLE t (`c` `UInt64`) ENGINE = Memory');

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103808
--
-- After PR #88770 enabled JIT compilation for `Decimal` types, `if(cond, decimal_col, int_literal)`
-- (and the symmetric `if(cond, int_literal, decimal_col)`, plus `multiIf` with mixed branches)
-- silently returned a value `10^scale` too small. The JIT codegen for `if`/`multiIf` called
-- `nativeCast` to convert the integer-literal branch to the `Decimal` result type, but
-- `nativeCast` only reinterprets the integer bits — it does not multiply by `10^scale`.
-- Each query below must produce the same result as the non-JIT path.

SET min_count_to_compile_expression = 0;
SET compile_expressions = 1;

-- Original den-crane reproducer.
WITH materialize(1::Decimal(18, 7)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1)) AS broken_sum;

-- Both branches of `if` exercised, integer literal in else.
WITH materialize(2::Decimal(18, 7)) AS r, materialize(0) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(2::Decimal(18, 7)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));

-- Integer literal in then branch.
WITH materialize(2::Decimal(18, 7)) AS r, materialize(1) AS k SELECT sum(if(k = 1, 1, r));
WITH materialize(2::Decimal(18, 7)) AS r, materialize(0) AS k SELECT sum(if(k = 1, 1, r));

-- `multiIf` with integer literals interleaved with a `Decimal` else branch.
WITH materialize(3::Decimal(18, 7)) AS r, materialize(1) AS k SELECT sum(multiIf(k = 1, 1, k = 2, 2, r));
WITH materialize(3::Decimal(18, 7)) AS r, materialize(2) AS k SELECT sum(multiIf(k = 1, 1, k = 2, 2, r));
WITH materialize(3::Decimal(18, 7)) AS r, materialize(0) AS k SELECT sum(multiIf(k = 1, 1, k = 2, 2, r));

-- All `Decimal` widths.
WITH materialize(1::Decimal(9, 4)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(18, 7)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(38, 7)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(38, 11)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));

-- High-scale `Decimal128`: `10^scale` exceeds 64 bits. The JIT helper builds the scale
-- factor as a `128`-bit `APInt`, so the multiplication has to stay correct without ever
-- narrowing to `uint64_t`.
WITH materialize(1::Decimal(38, 20)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(38, 30)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(38, 37)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(2::Decimal(38, 30)) AS r, materialize(1) AS k SELECT sum(if(k = 1, 1, r));
WITH materialize(3::Decimal(38, 30)) AS r, materialize(2) AS k SELECT sum(multiIf(k = 1, 1, k = 2, 2, r));

-- `Decimal256` high scale: `10^scale` exceeds even 128 bits, exercising the `256`-bit `APInt` path.
WITH materialize(1::Decimal(76, 60)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(76, 73)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));

-- `Float` -> `Decimal` branch lift (the analyzer promotes the result to `Decimal`).
WITH materialize(toDecimal64(1.5, 4)) AS r, materialize(1) AS k SELECT if(k != 1, r, 2.5::Float64);

-- Sanity check: the non-JIT path produces the same answers.
SET compile_expressions = 0;
WITH materialize(1::Decimal(18, 7)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(3::Decimal(18, 7)) AS r, materialize(2) AS k SELECT sum(multiIf(k = 1, 1, k = 2, 2, r));
WITH materialize(1::Decimal(38, 11)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(38, 30)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(38, 37)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));
WITH materialize(1::Decimal(76, 73)) AS r, materialize(1) AS k SELECT sum(if(k != 1, r, 1));

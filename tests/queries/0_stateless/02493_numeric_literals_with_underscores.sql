SELECT 1234; -- Positive integer (+ implied)
SELECT 1_234;
SELECT 1_2_3_4;
SELECT +1234; -- Positive integer (+ explicit)
SELECT +1_234;
SELECT +1_2_3_4;
SELECT -1234; -- Negative integer
SELECT -1_234;
SELECT -1_2_3_4;
SELECT 12.34; -- Positive floating point with . notation
SELECT 12.3_4;
SELECT 1_2.34;
SELECT 1_2.3_4;
SELECT -12.34; -- Negative floating point with . notation
SELECT -12.3_4;
SELECT -1_2.34;
SELECT -1_2.3_4;
SELECT 34e21; -- Positive floating point with positive scientific notation (+ implied)
SELECT 3_4e21;
SELECT 34e2_1;
SELECT 3_4e2_1;
SELECT 34e+21; -- Positive floating point with positive scientific notation (+ explicit)
SELECT 3_4e+21;
SELECT 34e+2_1;
SELECT 3_4e+2_1;
SELECT 34e-21; -- Positive floating point with negative scientific notation
SELECT 3_4e-21;
SELECT 34e-2_1;
SELECT 3_4e-2_1;
SELECT -34e21; -- Negative floating point with positive scientific notation (+ implied)
SELECT -3_4e21;
SELECT -34e2_1;
SELECT -3_4e2_1;
SELECT -34e+21; -- Negative floating point with positive scientific notation (+ explicit)
SELECT -3_4e+21;
SELECT -34e+2_1;
SELECT -3_4e+2_1;
SELECT -34e-21; -- Negative floating point with negative scientific notation
SELECT -3_4e-21;
SELECT -34e-2_1;
SELECT -3_4e-2_1;
SELECT 1.34e21; -- Positive floating point (with .) with positive scientific notation (+ implied)
SELECT 1.3_4e21;
SELECT 1.34e2_1;
SELECT 1.3_4e2_1;
SELECT 1.34e+21; -- Positive floating point (with .) with positive scientific notation (+ explicit)
SELECT 1.3_4e+21;
SELECT 1.34e+2_1;
SELECT 1.3_4e+2_1;
SELECT 1.34e-21; -- Positive floating point (with .) with negative scientific notation
SELECT 1.3_4e-21;
SELECT 1.34e-2_1;
SELECT 1.3_4e-2_1;
SELECT -1.34e21; -- Negative floating point (with .) with positive scientific notation (+ implied)
SELECT -1.3_4e21;
SELECT -1.34e2_1;
SELECT -1.3_4e2_1;
SELECT -1.34e+21; -- Negative floating point (with .) with positive scientific notation (+ explicit)
SELECT -1.3_4e+21;
SELECT -1.34e+2_1;
SELECT -1.3_4e+2_1;
SELECT -1.34e-21; -- Negative floating point (with .) with negative scientific notation
SELECT -1.3_4e-21;
SELECT -1.34e-2_1;
SELECT -1.3_4e-2_1;
SELECT -.34e21; -- Negative floating point (with .) with positive scientific notation (+ implied)
SELECT -.3_4e21;
SELECT -.34e2_1;
SELECT -.3_4e2_1;
SELECT -.34e+21; -- Negative floating point (with .) with positive scientific notation (+ explicit)
SELECT -.3_4e+21;
SELECT -.34e+2_1;
SELECT -.3_4e+2_1;
SELECT -.34e-21; -- Negative floating point (with .) with negative scientific notation
SELECT -.3_4e-21;
SELECT -.34e-2_1;
SELECT -.3_4e-2_1;
SELECT NaN; -- Specials
SELECT nan;
SELECT inf;
SELECT +inf;
SELECT -inf;
SELECT Inf;
SELECT +Inf;
SELECT -Inf;
SELECT INF;
SELECT +INF;
SELECT -INF;
SELECT 0b1111; -- Binary
SELECT 0b1_111;
SELECT 0b1_1_1_1;
SELECT -0b1111;
SELECT -0b1_111;
SELECT -0b1_1_1_1;
SELECT 0x1234; -- Hex
SELECT 0x1_234;
SELECT 0x1_2_3_4;
SELECT -0x1234;
SELECT -0x1_234;
SELECT -0x1_2_3_4;
SELECT 0xee;
SELECT 0xe_e;
SELECT 0x1.234; -- Hex fractions
SELECT 0x1.2_3_4;
SELECT -0x1.234;
SELECT -0x1.2_3_4;
SELECT 0x0.ee;
SELECT 0x0.e_e;
SELECT 0x1.234p01; -- Hex scientific notation
SELECT 0x1.2_34p01;
SELECT 0x1.234p0_1;
SELECT 0x1.234p+01;
SELECT 0x1.2_34p+01;
SELECT 0x1.2_34p+0_1;
SELECT 0x1.234p-01;
SELECT 0x1.2_34p-01;
SELECT 0x1.2_34p-0_1;
SELECT -0x1.234p01;
SELECT -0x1.2_34p01;
SELECT -0x1.2_34p0_1;
SELECT -0x1.234p+01;
SELECT -0x1.2_34p+01;
SELECT -0x1.2_34p+0_1;
SELECT -0x1.234p-01;
SELECT -0x1.2_34p-01;
SELECT -0x1.2_34p-0_1;

-- Things that are not a number

select _1000; -- { serverError UNKNOWN_IDENTIFIER }
select _1000 FROM (SELECT 1 AS _1000) FORMAT Null;
select -_1; -- { serverError UNKNOWN_IDENTIFIER }
select -_1 FROM (SELECT -1 AS _1) FORMAT Null;
select +_1; -- { serverError UNKNOWN_IDENTIFIER }
select 1__0; -- { serverError UNKNOWN_IDENTIFIER }
select 1_; -- { serverError UNKNOWN_IDENTIFIER }
select 1_ ; -- { serverError UNKNOWN_IDENTIFIER }
select 10_; -- { serverError UNKNOWN_IDENTIFIER }
select 1_e5; -- { serverError UNKNOWN_IDENTIFIER }
select 1e_5; -- { serverError UNKNOWN_IDENTIFIER }
select 1e5_; -- { serverError UNKNOWN_IDENTIFIER }
select 1e_; -- { serverError UNKNOWN_IDENTIFIER }
select 1_.; -- { clientError SYNTAX_ERROR }
select 1e_1; -- { serverError UNKNOWN_IDENTIFIER }
select 0_x2; -- { serverError UNKNOWN_IDENTIFIER }
select 0x2_p2; -- { serverError UNKNOWN_IDENTIFIER }
select 0x2p_2; -- { serverError UNKNOWN_IDENTIFIER }
select 0x2p2_; -- { serverError UNKNOWN_IDENTIFIER }
select 0b; -- { serverError UNKNOWN_IDENTIFIER }
select 0b ; -- { serverError UNKNOWN_IDENTIFIER }
select 0x; -- { serverError UNKNOWN_IDENTIFIER }
select 0x ; -- { serverError UNKNOWN_IDENTIFIER }
select 0x_; -- { serverError UNKNOWN_IDENTIFIER }
select 0x_1; -- { serverError UNKNOWN_IDENTIFIER }

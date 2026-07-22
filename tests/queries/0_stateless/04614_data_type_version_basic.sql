-- Basic tests for the `Version` data type (major.minor.patch.build packed into UInt128):
--   * parsing pads missing trailing components with 0, so a version with fewer than 4
--     components compares equal to its fully-specified canonical form;
--   * leading zeros in a component are parsed purely numerically ('01' -> 1);
--   * text output always prints the full canonical 4-component form, regardless of how
--     many components were given on input.
-- A clean run produces no output (all assertions pass).

SELECT throwIf(NOT (toVersion('1.2') = toVersion('1.2.0.0')), 'FAIL: padding equality 1.2 vs 1.2.0.0');
SELECT throwIf(NOT (toVersion('20.0') = toVersion('20.0.0.0')), 'FAIL: padding equality 20.0');
SELECT throwIf(NOT (toVersion('5') = toVersion('5.0.0.0')), 'FAIL: padding equality 5');
SELECT throwIf(toString(toVersion('1.2')) != '1.2.0.0', 'FAIL: canonical output form');
SELECT throwIf(toString(toVersion('1.2.3.4')) != '1.2.3.4', 'FAIL: full round-trip');
SELECT throwIf(toVersion('01.2.3.4') != toVersion('1.2.3.4'), 'FAIL: leading zero parsing');
SELECT throwIf(toTypeName(toVersion('1.0')) != 'Version', 'FAIL: type name');

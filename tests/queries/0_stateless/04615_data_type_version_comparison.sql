-- Comparison ordering for the Version data type, plus the two documented rejection cases.
--
-- A clean run of the throwIf() assertions below produces no output (throwIf raises only when
-- its condition is true, i.e. only on a genuine failure).
--
-- The last two statements are deliberately invalid input and are expected to throw
-- CANNOT_PARSE_VERSION; per the house convention used throughout this test suite (see e.g.
-- tests/queries/0_stateless/00725_ipv4_ipv6_domains.sql for CANNOT_PARSE_IPV4/CANNOT_PARSE_IPV6),
-- this is expressed with a `-- { serverError ... }` annotation on the same line rather than a
-- separate script/exit-code, so the whole file still produces a single overall pass/fail result.

-- Major dominates regardless of minor/patch/build.
SELECT throwIf(NOT (toVersion('2.0.0.0') > toVersion('1.99.99.99')), 'FAIL: major dominates');

-- Minor dominates when major is equal.
SELECT throwIf(NOT (toVersion('1.5.0.0') > toVersion('1.4.99.99')), 'FAIL: minor dominates');

-- Patch dominates when major and minor are equal.
SELECT throwIf(NOT (toVersion('1.2.5.0') > toVersion('1.2.4.99')), 'FAIL: patch dominates');

-- Build dominates when major, minor and patch are equal.
SELECT throwIf(NOT (toVersion('1.2.3.5') > toVersion('1.2.3.4')), 'FAIL: build dominates');

-- Equality of fully-specified equal versions.
SELECT throwIf(NOT (toVersion('1.2.3.4') = toVersion('1.2.3.4')), 'FAIL: equality');

-- Inequality (!=).
SELECT throwIf(toVersion('1.2.3.4') = toVersion('1.2.3.5'), 'FAIL: inequality (!=)');
SELECT throwIf(NOT (toVersion('1.2.3.4') != toVersion('1.2.3.5')), 'FAIL: != spot check');

-- <=, >= spot checks (both the strict and the equal side).
SELECT throwIf(NOT (toVersion('1.2.3.4') <= toVersion('1.2.3.5')), 'FAIL: <= strict side');
SELECT throwIf(NOT (toVersion('1.2.3.4') <= toVersion('1.2.3.4')), 'FAIL: <= equal side');
SELECT throwIf(NOT (toVersion('1.2.3.5') >= toVersion('1.2.3.4')), 'FAIL: >= strict side');
SELECT throwIf(NOT (toVersion('1.2.3.4') >= toVersion('1.2.3.4')), 'FAIL: >= equal side');

-- The following statement is expected to throw (invalid component 'abc' is non-numeric); this is intentional and documented behavior:
SELECT toVersion('1.2.abc'); -- { serverError CANNOT_PARSE_VERSION }

-- The following statement is expected to throw (5 components exceeds the max of 4); this is intentional and documented behavior:
SELECT toVersion('1.2.3.4.5'); -- { serverError CANNOT_PARSE_VERSION }

-- Regression test for the cross-version compatibility guarantee of `SettingFieldMaxThreads`.
--
-- Context: https://github.com/ClickHouse/ClickHouse/pull/108657
--          https://github.com/ClickHouse/ClickHouse/issues/68748
--
-- `system.settings` now renders the automatic value of `max_threads` (and friends) as the clean
-- `auto(N)` form instead of the legacy `'auto(N)'` form that had the single quotes baked into the
-- string value. That rendering change is only safe because `stringToMaxThreads` keeps accepting
-- BOTH forms when parsing: settings travel between servers as their string representation and are
-- re-parsed on the receiving side, so an older replica still sends the quoted `'auto(N)'` over the
-- wire and a new server must understand it.
--
-- This test pins that parsing contract directly. Removing the `startsWith(str, "'auto")` branch in
-- `stringToMaxThreads` would make `SET max_threads = '''auto(8)'''` throw a parse error (the value
-- `'auto(8)'` is not a number), so this test would fail loudly - which is exactly the mixed-version
-- regression we want to guard against.
--
-- We assert on `startsWith(value, 'auto(')` and `position(value, '''') = 0` rather than the exact
-- core count, so the test does not depend on the machine it runs on.

-- 1. The clean `auto(N)` form is accepted and stays auto.
SET max_threads = 'auto(8)';
SELECT 'clean-form is-auto',   startsWith(value, 'auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'clean-form no-quotes', position(value, '''') = 0  FROM system.settings WHERE name = 'max_threads';
SELECT 'clean-form resolved',  getSetting('max_threads') > 0;

-- 2. The legacy quoted `'auto(N)'` form (the literal string an older replica sends) is also accepted
--    and stays auto. The triple-quoted SQL literal produces the string value `'auto(8)'`, quotes
--    included. This is the branch the reviewer flagged as untested.
SET max_threads = '''auto(8)''';
SELECT 'legacy-form is-auto',   startsWith(value, 'auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'legacy-form no-quotes', position(value, '''') = 0  FROM system.settings WHERE name = 'max_threads';
SELECT 'legacy-form resolved',  getSetting('max_threads') > 0;

-- 3. The same contract holds for the other `SettingFieldMaxThreads` settings, since they share the
--    `stringToMaxThreads` parsing path.
SET max_final_threads = '''auto(8)''';
SELECT 'max_final_threads legacy is-auto', startsWith(value, 'auto(') FROM system.settings WHERE name = 'max_final_threads';
SET max_parsing_threads = 'auto(8)';
SELECT 'max_parsing_threads clean is-auto', startsWith(value, 'auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 4. An explicit numeric value passed as a string is still parsed as a number, not auto.
SET max_threads = '16';
SELECT 'explicit is-auto', startsWith(value, 'auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'explicit value',   value FROM system.settings WHERE name = 'max_threads';

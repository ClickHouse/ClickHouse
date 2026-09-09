-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103120
--
-- `SET <name> = DEFAULT` used to lose the `is_auto` flag for `SettingFieldMaxThreads`-typed
-- settings whose declared default is `0` (auto): `max_threads`, `max_final_threads`,
-- `max_parsing_threads`. The cause was that `BaseSettings::resetValueToDefault` round-trips
-- through `static_cast<Field>(*default)`, and `SettingFieldMaxThreads::operator Field` returns
-- the resolved auto-value (an opaque `UInt64`) rather than the canonical `0`/`"auto"`, because
-- it deliberately behaves like a plain `UInt64` for backward compatibility with code that reads
-- `getSetting('max_threads')` as numeric. The subsequent `operator=(const Field &)` therefore
-- reconstructed the field as if it had been set to that explicit value, dropping `is_auto`.
--
-- The fix adds `SettingFieldBase::resetFromDefault(const SettingFieldBase &)` (default
-- implementation goes through `Field`, matching the legacy behaviour for every type whose
-- `operator Field` is invertible) and overrides it in `SettingFieldMaxThreads` to copy the
-- typed default member-wise. `BaseSettings::resetValueToDefault` now dispatches through this
-- virtual, so each type can choose whether the `Field` round-trip is safe for it. `operator
-- Field` is unchanged, so distributed forwarding and code that reads `max_threads` as `UInt64`
-- retain their previous behaviour.
--
-- The `SettingFieldMaxThreads` family contains four settings with two distinct kinds of declared
-- defaults, both of which must be preserved across `SET <name> = DEFAULT`:
--   * `max_threads`, `max_final_threads`, `max_parsing_threads`  - declared default `0` (auto)
--   * `max_download_threads`                                     - declared default `4` (explicit)
-- The previous attempt to fix this by overriding `setChanged(false)` to hard-code the auto state
-- regressed `max_download_threads` to `'auto(N)'` instead of `4`. This test pins the spec for both
-- groups and adds `getSetting` checks so the runtime `Field` type and value are also asserted.
--
-- We use `startsWith(value, '''auto(')` (single-quoted because `system.settings.value` quotes the
-- string itself) to assert that the auto state is preserved without depending on the actual core
-- count.

-- 1. Initial state: undo any session-level value the test runner may have injected via
--    randomized settings, then assert auto for the three auto-defaulted settings. This step
--    itself exercises the fix - on a buggy build the reset would return the resolved value
--    instead of auto.
SET max_threads = DEFAULT;
SET max_final_threads = DEFAULT;
SET max_parsing_threads = DEFAULT;
SELECT 'max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 2. Setting an explicit value clears auto.
SET max_threads = 4;
SET max_final_threads = 5;
SET max_parsing_threads = 6;
SELECT 'after-set max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'after-set max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'after-set max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 3. SET = DEFAULT must restore auto for all three settings.
SET max_threads = DEFAULT;
SET max_final_threads = DEFAULT;
SET max_parsing_threads = DEFAULT;
SELECT 'after-default max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'after-default max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'after-default max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 4. Resetting an already-auto setting must keep it auto.
SET max_threads = DEFAULT;
SELECT 'idempotent max_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';

-- 5. Multiple-setting form `SET a = DEFAULT, b = DEFAULT` must restore auto for all.
SET max_threads = 7, max_final_threads = 8, max_parsing_threads = 9;
SET max_threads = DEFAULT, max_final_threads = DEFAULT, max_parsing_threads = DEFAULT;
SELECT 'multi-default max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'multi-default max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'multi-default max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 6. `max_download_threads` is the only `SettingFieldMaxThreads` setting with a non-zero declared
--    default (`4`). Its DEFAULT must restore the explicit value, NOT auto. The earlier
--    `setChanged`-based fix attempt regressed exactly this case.
SET max_download_threads = DEFAULT;
SELECT 'max_download_threads default',
       startsWith(value, '''auto(') AS is_auto, value
FROM system.settings WHERE name = 'max_download_threads';

SET max_download_threads = 1;
SELECT 'max_download_threads after-set', value FROM system.settings WHERE name = 'max_download_threads';

SET max_download_threads = DEFAULT;
SELECT 'max_download_threads after-default',
       startsWith(value, '''auto(') AS is_auto, value
FROM system.settings WHERE name = 'max_download_threads';

-- 7. `max_download_threads = 0` should switch to auto (the `SettingFieldMaxThreads` convention),
--    and the subsequent DEFAULT must still restore the declared `4` - it must NOT remember "this
--    setting is currently auto" and stay auto.
SET max_download_threads = 0;
SELECT 'max_download_threads after-zero', startsWith(value, '''auto(')
FROM system.settings WHERE name = 'max_download_threads';

SET max_download_threads = DEFAULT;
SELECT 'max_download_threads after-default-from-auto',
       startsWith(value, '''auto(') AS is_auto, value
FROM system.settings WHERE name = 'max_download_threads';

-- 8. `getSetting` must return a numeric (`UInt`-family) `Field` for every `SettingFieldMaxThreads`
--    setting in every state. This is the backward-compat invariant the AI reviewer flagged on the
--    previous `operator Field` redesign attempt: callers that read `max_threads` as `UInt64` must
--    not see `String('auto')`. We test type-prefix rather than exact type because the analyzer
--    narrows numeric literals to the smallest fitting `UInt` type (`UInt8`/`UInt16`/`UInt64`),
--    which depends on the resolved auto value.
SET max_threads = DEFAULT;
SELECT 'getSetting numeric max_threads default',         startsWith(toTypeName(getSetting('max_threads')), 'UInt');
SELECT 'getSetting > 0 max_threads default',             getSetting('max_threads') > 0;

SET max_threads = 4;
SELECT 'getSetting numeric max_threads explicit',        startsWith(toTypeName(getSetting('max_threads')), 'UInt');
SELECT 'getSetting value max_threads explicit',          getSetting('max_threads');

SET max_final_threads = DEFAULT;
SELECT 'getSetting numeric max_final_threads default',   startsWith(toTypeName(getSetting('max_final_threads')), 'UInt');
SELECT 'getSetting > 0 max_final_threads default',       getSetting('max_final_threads') > 0;

SET max_parsing_threads = DEFAULT;
SELECT 'getSetting numeric max_parsing_threads default', startsWith(toTypeName(getSetting('max_parsing_threads')), 'UInt');
SELECT 'getSetting > 0 max_parsing_threads default',     getSetting('max_parsing_threads') > 0;

SET max_download_threads = DEFAULT;
SELECT 'getSetting numeric max_download_threads default', startsWith(toTypeName(getSetting('max_download_threads')), 'UInt');
SELECT 'getSetting value max_download_threads default',   getSetting('max_download_threads');

-- 9. Control: a `SettingAutoWrapper`-style setting (`query_plan_join_swap_table`) preserves the
--    literal `auto` keyword across DEFAULT - was already correct, must remain correct.
SET query_plan_join_swap_table = 'true';
SELECT 'autowrapper-set', value FROM system.settings WHERE name = 'query_plan_join_swap_table';
SET query_plan_join_swap_table = DEFAULT;
SELECT 'autowrapper-default', value FROM system.settings WHERE name = 'query_plan_join_swap_table';

-- 10. Control: a regular numeric setting (`max_block_size`) is `changed=0` after DEFAULT, and its
--     `Field` round-trip is invertible - must not regress.
SET max_block_size = 12345;
SELECT 'control changed', changed FROM system.settings WHERE name = 'max_block_size';
SET max_block_size = DEFAULT;
SELECT 'control default-changed', changed FROM system.settings WHERE name = 'max_block_size';
SELECT 'control numeric', startsWith(toTypeName(getSetting('max_block_size')), 'UInt');

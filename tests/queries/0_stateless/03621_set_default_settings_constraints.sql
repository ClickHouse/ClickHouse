-- Regression test for a settings-constraint bypass via "SET <setting> = DEFAULT".
-- Resetting a setting to its default value must honor the same constraints
-- (readonly mode, const/min/max) as an explicit assignment. Previously the
-- reset-to-default path skipped constraint checks, so a read-only-sandboxed user
-- could escape by running "SET readonly = DEFAULT".

-- A normal reset to default still works when the setting is unconstrained.
SET max_threads = 100;
SET max_threads = DEFAULT;

-- Self-impose read-only mode for the rest of the session.
SET readonly = 1;

-- Value assignment under readonly is rejected (pre-existing behavior, used here as a control).
SET max_threads = 4; -- { serverError READONLY }

-- Reset to default under readonly must be rejected as well (the behavior fixed here).
SET max_threads = DEFAULT; -- { serverError READONLY }

-- The readonly setting itself must not be resettable to escape the sandbox.
SET readonly = DEFAULT; -- { serverError READONLY }

-- The read-only sandbox is still in effect.
SELECT getSetting('readonly');

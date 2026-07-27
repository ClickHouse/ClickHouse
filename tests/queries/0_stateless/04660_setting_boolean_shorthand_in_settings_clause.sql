-- The value-less `name` form of a setting change stands for `name = true`, so it only makes sense
-- for a Bool setting. The parser accepts it for any name - it does not know the settings schema -
-- and the type is checked when the change is applied, which for a `SETTINGS` clause happens on a
-- different path than for a standalone `SET`.

SELECT count() FROM numbers(3) SETTINGS optimize_move_to_prewhere;

SELECT 1 SETTINGS max_threads; -- { error TYPE_MISMATCH }

-- An enum-valued setting must be reported as a type mismatch, not as a `BAD_GET` from casting
-- `true` to its type while checking the constraints.
SELECT 1 SETTINGS default_database_engine; -- { error TYPE_MISMATCH }

SELECT 1 SETTINGS this_setting_does_not_exist; -- { error UNKNOWN_SETTING }

-- The rejected shorthand must not leave the setting at whatever `true` converts to.
SET max_threads = 8;
SET max_threads; -- { serverError TYPE_MISMATCH }
SELECT getSetting('max_threads');

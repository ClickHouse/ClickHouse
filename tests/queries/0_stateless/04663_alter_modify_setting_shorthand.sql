-- A value-less `MODIFY SETTING name` is shorthand for `= true` and is only valid for a Bool
-- setting. The check must also fire when the setting is already present in the table metadata:
-- the merge into the stored settings has to keep the shorthand marker rather than only the value,
-- or `index_granularity` below would silently become 1.

DROP TABLE IF EXISTS t_setting_shorthand;
CREATE TABLE t_setting_shorthand (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS index_granularity = 8192, replace_long_file_name_to_hash = false;

ALTER TABLE t_setting_shorthand MODIFY SETTING index_granularity; -- { serverError TYPE_MISMATCH }

-- For a Bool setting the shorthand is accepted, including when it overwrites a stored entry.
ALTER TABLE t_setting_shorthand MODIFY SETTING replace_long_file_name_to_hash;

-- The rejected shorthand left `index_granularity` alone; the accepted one flipped the Bool.
SHOW CREATE TABLE t_setting_shorthand;

DROP TABLE t_setting_shorthand;

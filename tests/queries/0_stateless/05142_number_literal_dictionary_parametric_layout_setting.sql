-- A dictionary whose layout takes parameters writes its `SETTINGS(...)` values into a second place,
-- the root `settings` block of the generated configuration. A literal too large for UInt64 resolves
-- to a wide integer, and quoting it there made the dictionary fail to load with
-- `Cannot read floating point value here`, while the same settings clause worked for a layout
-- without parameters.

DROP DICTIONARY IF EXISTS dict_parametric_layout_setting;
DROP TABLE IF EXISTS dict_parametric_layout_source;

CREATE TABLE dict_parametric_layout_source (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dict_parametric_layout_source VALUES (1, 2);

CREATE DICTIONARY dict_parametric_layout_setting
(
    k UInt64,
    v UInt64
)
PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'dict_parametric_layout_source' DB currentDatabase()))
LAYOUT(CACHE(SIZE_IN_CELLS 50))
LIFETIME(0)
SETTINGS(totals_auto_threshold = 18446744073709551616);

SELECT dictGet('dict_parametric_layout_setting', 'v', toUInt64(1));

DROP DICTIONARY dict_parametric_layout_setting;
DROP TABLE dict_parametric_layout_source;

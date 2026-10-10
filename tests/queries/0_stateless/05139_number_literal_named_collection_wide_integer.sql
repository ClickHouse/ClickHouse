-- A named collection value and a dictionary setting value are not SQL literals: they are read back
-- as bare text. A literal too large for UInt64 resolves to a wide integer, and quoting it turns the
-- value into a String, so the formatted query no longer means the same thing and a dictionary with
-- such a setting fails to load.

SELECT formatQuery('CREATE NAMED COLLECTION nc AS a = 18446744073709551616, b = 3.5, c = 42') AS create_named_collection;
SELECT formatQuery('ALTER NAMED COLLECTION nc SET a = 18446744073709551616') AS alter_named_collection;

WITH 'CREATE DICTIONARY dict (k UInt64, v UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(QUERY $$SELECT 1, 2$$)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(totals_auto_threshold = 18446744073709551616)' AS q
SELECT
    splitByChar('\n', formatQuery(q))[-1] AS dictionary_settings,
    JSONExtractString(parseQueryToJSON(q), 'dictionary', 'dict_settings', 'changes', 1, 'value', 'field_type') AS before,
    JSONExtractString(parseQueryToJSON(formatQuery(q)), 'dictionary', 'dict_settings', 'changes', 1, 'value', 'field_type') AS after;

DROP DICTIONARY IF EXISTS dict_wide_integer_setting;

CREATE DICTIONARY dict_wide_integer_setting
(
    k UInt64,
    v UInt64
)
PRIMARY KEY k
SOURCE(CLICKHOUSE(QUERY 'SELECT 1, 2'))
LAYOUT(FLAT())
LIFETIME(0)
SETTINGS(totals_auto_threshold = 18446744073709551616);

SELECT dictGet('dict_wide_integer_setting', 'v', toUInt64(1)) AS dict_get;

DROP DICTIONARY dict_wide_integer_setting;

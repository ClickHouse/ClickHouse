-- The name of a dictionary setting is read with `ParserCompoundIdentifier`, which accepts a
-- back-quoted string, so the name can hold any byte. `ASTDictionarySettings::formatImpl` wrote it
-- with no quoting, so a name that is not an identifier came back as a different token sequence and
-- the formatted definition no longer parsed. A dictionary definition is stored in formatted form, so
-- such a name also made the next server start fail with
-- `Cannot parse definition from metadata file ... (SYNTAX_ERROR)`.
--
-- Each case prints the formatted definition and whether formatting it again is a fixed point.

-- Whitespace in the name.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`my setting` = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- Punctuation in the name.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`a-b` = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- Two changes, so the separator between them is covered.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`a b` = 1, `c d` = 2)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- A name outside ASCII.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`ключ` = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- Control characters. Back quotes escape them, so no byte class needs a rule of its own.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`a\nb` = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`a\0b` = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- A plain setting name, and a compound one, keep the unquoted spelling every existing dictionary
-- definition carries.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(max_threads = 8)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(a.b = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- A name whose last dot-separated part is empty. Formatting it as the remaining parts is a fixed
-- point of a shorter name, so the printed name, not the fixed point, is what this case asserts.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`a.` = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`a.b.` = 1)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- An empty first part, and an empty part between two others, keep the quoting they already had.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`.a` = 1, `a..b` = 2)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- A realistic definition, which also shows that the key, source and layout names are formatted as
-- before.
WITH 'CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host ''localhost'' port 9000 table ''ids'')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(MIN 1 MAX 10) SETTINGS(max_threads = 8)' AS q
SELECT formatQuerySingleLine(q), formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

-- An empty name has no spelling the parser accepts, so the AST JSON reader rejects it instead of
-- building a dictionary whose definition cannot be read back.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(sn = 1)'), '"name":"sn"', '"name":""')); -- { serverError BAD_ARGUMENTS }

-- A settings clause with no settings has no spelling either, since the parser requires at least one
-- pair, so the reader rejects it rather than formatting `SETTINGS()`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(sn = 1)'), '"changes":', '"changes_absent":')); -- { serverError BAD_ARGUMENTS }

-- The definition regenerated from the AST is what is stored in the metadata file, and `ATTACH` reads
-- it back the way a server start does.
DROP DICTIONARY IF EXISTS dict_setting_name;
CREATE DICTIONARY dict_setting_name (k UInt64, v UInt64) PRIMARY KEY k
SOURCE(CLICKHOUSE(host 'localhost')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(`my setting` = 1);

SELECT create_table_query LIKE '%`my setting`%' FROM system.tables
WHERE database = currentDatabase() AND name = 'dict_setting_name';

DETACH DICTIONARY dict_setting_name;
ATTACH DICTIONARY dict_setting_name;

SELECT count() FROM system.dictionaries
WHERE database = currentDatabase() AND name = 'dict_setting_name';

DROP DICTIONARY dict_setting_name;

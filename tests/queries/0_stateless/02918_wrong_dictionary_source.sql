DROP DICTIONARY IF EXISTS id_value_dictionary;
DROP TABLE IF EXISTS source_table;

CREATE TABLE source_table(id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();

-- There is no "CLICKHOUSEX" dictionary source, so the next query must fail even if `dictionaries_lazy_load` is enabled.
CREATE DICTIONARY id_value_dictionary(id UInt64, value String) PRIMARY KEY id SOURCE(CLICKHOUSEX(TABLE 'source_table')) LIFETIME(MIN 0 MAX 1000) LAYOUT(FLAT()); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

SELECT count() FROM system.dictionaries WHERE name=='id_value_dictionary' AND database==currentDatabase();

DROP TABLE source_table;

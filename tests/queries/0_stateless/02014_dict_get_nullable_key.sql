DROP TABLE IF EXISTS dictionary_non_nullable_source_table;
CREATE TABLE dictionary_non_nullable_source_table (id UInt64, value String) ENGINE=TinyLog;
INSERT INTO dictionary_non_nullable_source_table VALUES (0, 'Test');

DROP DICTIONARY IF EXISTS test_dictionary_non_nullable;
CREATE DICTIONARY test_dictionary_non_nullable (id UInt64, value String) PRIMARY KEY id LAYOUT(DIRECT()) SOURCE(CLICKHOUSE(TABLE 'dictionary_non_nullable_source_table'));

SELECT 'Non nullable value only null key ';
SELECT dictGet('test_dictionary_non_nullable', 'value', NULL);
SELECT 'Non nullable value nullable key';
SELECT dictGet('test_dictionary_non_nullable', 'value', arrayJoin([toUInt64(0), NULL, 1]));

DROP DICTIONARY test_dictionary_non_nullable;
DROP TABLE dictionary_non_nullable_source_table;

DROP TABLE IF EXISTS dictionary_nullable_source_table;
CREATE TABLE dictionary_nullable_source_table (id UInt64, value Nullable(String)) ENGINE=TinyLog;
INSERT INTO dictionary_nullable_source_table VALUES (0, 'Test'), (1, NULL);

DROP DICTIONARY IF EXISTS test_dictionary_nullable;
CREATE DICTIONARY test_dictionary_nullable (id UInt64, value Nullable(String)) PRIMARY KEY id LAYOUT(DIRECT()) SOURCE(CLICKHOUSE(TABLE 'dictionary_nullable_source_table'));

SELECT 'Nullable value only null key ';
SELECT dictGet('test_dictionary_nullable', 'value', NULL);
SELECT 'Nullable value nullable key';
SELECT dictGet('test_dictionary_nullable', 'value', arrayJoin([toUInt64(0), NULL, 1, 2]));

DROP DICTIONARY test_dictionary_nullable;
DROP TABLE dictionary_nullable_source_table;

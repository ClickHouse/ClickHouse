CREATE DICTIONARY existing_ddl_dictionary_with_insert_query
(
    `id` UInt64,
    `SomeValue1` UInt8,
    `SomeValue2` String
)
PRIMARY KEY id
LAYOUT(FLAT())
SOURCE(CLICKHOUSE(QUERY `INSERT INTO test.xml_dictionary_table values (987123, 30, "sv")`))
LIFETIME(MIN 0 MAX 0)

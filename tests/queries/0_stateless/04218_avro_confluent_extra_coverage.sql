-- Tags: no-fasttest

SELECT * FROM format(AvroConfluent, 'a Int32', 'somedata'); -- { serverError BAD_ARGUMENTS }

SELECT * FROM format(AvroConfluent, 'a Int32', 'somedata') SETTINGS format_avro_schema_registry_url = ''; -- { serverError BAD_ARGUMENTS }

-- The schema reader for `AvroConfluent` reads the magic byte and schema id from the
-- input *before* contacting the registry. Empty input fails with `INCORRECT_DATA`
-- ("Missing AvroConfluent magic byte or schema identifier.").
DESC format(AvroConfluent, ''); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- A non-zero magic byte is rejected as "Invalid magic byte before AvroConfluent
-- schema identifier." -- 'x' is 0x78, not 0x00.
DESC format(AvroConfluent, 'xxxxxx'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }


-- AVRO_NULL deserialization coverage
INSERT INTO FUNCTION file(currentDatabase() || '_data_04218_null.avro') SELECT NULL::Nullable(Nothing) AS x FROM numbers(3) SETTINGS engine_file_truncate_on_insert=1;
DESC file(currentDatabase() || '_data_04218_null.avro');

-- Target `Nullable(UInt32)`: the AVRO_NULL branch returns a lambda that calls
-- `decoder.decodeNull` and `col.insertDefault` on the Nullable column.
SELECT * FROM file(currentDatabase() || '_data_04218_null.avro', auto, 'x Nullable(UInt32)');

-- Target `UInt32` with `input_format_null_as_default = 1`: AVRO_NULL falls through
-- to the `null_as_default` branch.
SELECT * FROM file(currentDatabase() || '_data_04218_null.avro', auto, 'x UInt32') SETTINGS input_format_null_as_default = 1;

-- Target `UInt32` without `null_as_default`: AVRO_NULL with non-nullable target
-- throws `BAD_ARGUMENTS` ("Cannot insert Avro Null into non-nullable type ...").
SELECT * FROM file(currentDatabase() || '_data_04218_null.avro', auto, 'x UInt32') SETTINGS input_format_null_as_default = 0; -- { serverError BAD_ARGUMENTS }


-- AVRO_MAP coverage in `createSkipFn`. When an Avro field is present in the file
-- but absent from the target structure, the deserializer builds a skip function
-- for it. A Map column drives the `case avro::AVRO_MAP:` branch of `createSkipFn`.
INSERT INTO FUNCTION file(currentDatabase() || '_data_04218_map.avro') SELECT number AS a, map(concat('k_', toString(number)), number) AS m FROM numbers(3) SETTINGS engine_file_truncate_on_insert=1;

-- Inferred schema includes both columns.
DESC file(currentDatabase() || '_data_04218_map.avro');

-- Project only `a` -- the map column `m` is skipped, exercising the AVRO_MAP skip
-- function.
SELECT * FROM file(currentDatabase() || '_data_04218_map.avro', auto, 'a UInt64') ORDER BY a;

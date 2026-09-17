-- Schema inference used to return a zero-column structure when header detection produced a
-- header whose every name unescaped to the empty string: the emptiness guard runs before the
-- filter that drops empty-named columns. `DESCRIBE` then answered with no columns at all, and
-- reading the data reached `data_types.at(0)` on an empty vector in
-- `RowInputFormatWithNamesAndTypes::readPrefix()` -> `std::out_of_range` (`Code: 1001`, which
-- aborts the server in a debug or sanitizer build).

-- CSV: `readCSVField` keeps the quotes so the field infers as String, but the name unescapes to "".
SELECT * FROM format(CSV, '""\n42\n'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESCRIBE format(CSV, '""\n42\n'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- TSV: `\N` unescapes to "" as well (the escaping rule maps it to the empty string).
SELECT * FROM format(TSV, '\\N\\N\n42\n'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- The shape the AST fuzzer hit: a non-default NULL representation makes a raw `\N` field infer as
-- String, so the first row is taken as the header.
SELECT * FROM format(TSV, '\\N\n42\n') SETTINGS format_tsv_null_representation = '<NULL>'; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Only columns whose name is empty are dropped; a header keeping at least one name still works.
DESCRIBE format(CSV, 'a,""\n1,2\n');
SELECT * FROM format(CSV, 'a,""\n1,2\n');

-- A single row is never taken as a header, so the column is named `c1` and nothing is dropped.
DESCRIBE format(CSV, '""');
-- With the default NULL representation the first row infers as NULL rather than String, so it is not taken as a header either.
DESCRIBE format(TSV, '\\N\n42\n');

-- `format()` never populates the schema cache, but `file()` does, and a cache hit returns the stored
-- structure without re-running inference. A cached entry is used only when the file is older than the
-- second its entry was registered in, so the reads below need the sleep to exercise the cache at all.
INSERT INTO FUNCTION file(currentDatabase() || '_05218_all_empty.csv', 'RawBLOB') SELECT '""\n42\n' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05218_one_empty.csv', 'RawBLOB') SELECT 'a,""\n1,2\n' SETTINGS engine_file_truncate_on_insert = 1;
SELECT sleep(1) FORMAT Null;

DESCRIBE file(currentDatabase() || '_05218_all_empty.csv'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESCRIBE file(currentDatabase() || '_05218_all_empty.csv'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE file(currentDatabase() || '_05218_one_empty.csv');
DESCRIBE file(currentDatabase() || '_05218_one_empty.csv');
-- An empty column name reaching the analyzer aborts the server, so reading the cached structure back
-- is the strongest assertion in this file.
SELECT * FROM file(currentDatabase() || '_05218_one_empty.csv');

-- A caller that passes its own structure needs only the detected format name, so an inferred
-- structure that is unusable on its own must not fail the read. The `DESCRIBE` pins that this file's
-- detected format does infer nothing but empty names, so the case cannot rot into a vacuous one.
INSERT INTO FUNCTION file(currentDatabase() || '_05218_format_only', 'RawBLOB') SELECT '{"": [1, 2]}' SETTINGS engine_file_truncate_on_insert = 1;
DESCRIBE file(currentDatabase() || '_05218_format_only'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
-- The read is wrapped in a subquery to keep it off the count-from-metadata fast paths, which answer
-- from the cached row count of the whole file rather than from the requested column.
SELECT count() FROM (SELECT * FROM file(currentDatabase() || '_05218_format_only', auto, 'a Nullable(Int64)'));

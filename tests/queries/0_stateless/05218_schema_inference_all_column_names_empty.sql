-- Schema inference used to return a zero-column structure when header detection produced a
-- header whose every name unescaped to the empty string: the emptiness guard runs before the
-- filter that drops empty-named columns. `DESCRIBE` then answered with no columns at all, and
-- reading the data reached `data_types.at(0)` on an empty vector in
-- `RowInputFormatWithNamesAndTypes::readPrefix()` -> `std::out_of_range` (`Code: 1001`, an abort
-- in a build with `abort_on_logical_error`).

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
DESCRIBE format(TSV, '\\N\n42\n');

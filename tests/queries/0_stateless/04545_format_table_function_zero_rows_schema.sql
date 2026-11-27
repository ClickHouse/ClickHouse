-- Regression test for issue #111390: the `format` table function used to return
-- a schema-less block (0 columns) when the input parsed to zero rows, so any
-- column reference failed with `NOT_FOUND_COLUMN_IN_BLOCK` even though `DESCRIBE`
-- reported the columns. The result must keep the column structure with 0 rows.

-- Explicit structure, empty input: the minimal repro.
SELECT * FROM format(JSONEachRow, 'a UInt32', '') FORMAT TSVWithNamesAndTypes;
SELECT count() FROM format(JSONEachRow, 'a UInt32', '');
DESCRIBE format(JSONEachRow, 'a UInt32', '');
-- Referencing a column no longer throws; it returns zero rows.
SELECT a FROM format(JSONEachRow, 'a UInt32', '') WHERE a > 0;

-- Format-agnostic: the same holds for other input formats.
SELECT * FROM format(TSV, 'a UInt32, b String', '') FORMAT TSVWithNamesAndTypes;
SELECT count() FROM format(Values, 'x UInt32', '');

-- Inferred (not explicit) structure that yields a header but zero data rows.
SELECT * FROM format(JSONCompactEachRowWithNamesAndTypes, '["a"]\n["UInt32"]\n') FORMAT TSVWithNamesAndTypes;

-- GeoJSON empty FeatureCollection: the motivating fuzzer case.
SELECT * FROM format(GeoJSON, '{"type":"FeatureCollection","features":[]}') FORMAT TSVWithNamesAndTypes;
SELECT count() FROM format(GeoJSON, '{"type":"FeatureCollection","features":[]}');

-- Non-empty input keeps working (the `concatenateBlocks` path is unchanged).
SELECT * FROM format(JSONEachRow, 'a UInt32', '{"a":1}\n{"a":2}') ORDER BY a;

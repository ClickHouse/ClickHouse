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
DESCRIBE file(currentDatabase() || '_05218_one_empty.csv') /* 05218 warm default */;
-- An empty column name reaching the analyzer aborts the server, so reading the cached structure back
-- is the strongest assertion in this file.
SELECT * FROM file(currentDatabase() || '_05218_one_empty.csv');

-- The reads above expect the same answer whether or not the cache was consulted, so they cannot tell a
-- hit from a miss. These two look at the cache itself. `number_of_rows` is cached even for a file whose
-- structure is not, so a row with a NULL schema is legitimate and must not be counted.
SELECT DISTINCT schema FROM system.schema_inference_cache
WHERE storage = 'File' AND source LIKE '%_05218_one_empty.csv%' ORDER BY 1;
SELECT count() FROM system.schema_inference_cache
WHERE storage = 'File' AND source LIKE '%_05218_all_empty.csv%' AND schema IS NOT NULL;

-- UNION mode merges the per-file structures by column name and rejects a name whose types have no
-- common type, and that merge runs before empty names are dropped. A file taken from the cache
-- contributes the stored structure, so a cached file and a freshly inferred one must contribute the
-- same way: otherwise the same query fails on a cold cache and succeeds on a warm one. Below, `''` is
-- the only name the two files share and its two types have no common type, so a contribution that
-- still carries `''` collides over a column that is about to be dropped anyway. The first read is cold
-- (nothing is cached yet) and, because it runs a second after the files were written, it leaves usable
-- entries behind, which makes the second read a hit.
INSERT INTO FUNCTION file(currentDatabase() || '_05218_union/A', 'RawBLOB') SELECT '{"": [1], "x": [1]}' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05218_union/B', 'RawBLOB') SELECT '{"": [[1]]}' SETTINGS engine_file_truncate_on_insert = 1;
SELECT sleep(1) FORMAT Null;
DESCRIBE file(currentDatabase() || '_05218_union/*', 'JSONColumns') /* 05218 cold union */ SETTINGS schema_inference_mode = 'union';
DESCRIBE file(currentDatabase() || '_05218_union/*', 'JSONColumns') /* 05218 warm union */ SETTINGS schema_inference_mode = 'union';

-- A caller that passes its own structure needs only the detected format name, so an inferred
-- structure that is unusable on its own must not fail the read. The `DESCRIBE` pins that this file's
-- detected format does infer nothing but empty names, so the case cannot rot into a vacuous one.
INSERT INTO FUNCTION file(currentDatabase() || '_05218_format_only', 'RawBLOB') SELECT '{"": [1, 2]}' SETTINGS engine_file_truncate_on_insert = 1;
DESCRIBE file(currentDatabase() || '_05218_format_only'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
-- The read is wrapped in a subquery to keep it off the count-from-metadata fast paths, which answer
-- from the cached row count of the whole file rather than from the requested column.
SELECT count() FROM (SELECT * FROM file(currentDatabase() || '_05218_format_only', auto, 'a Nullable(Int64)'));
-- The `*Cluster` twin resolves the format name the same way, through a separate call site.
SELECT count() FROM (SELECT * FROM fileCluster('test_cluster_two_shards_localhost',
    currentDatabase() || '_05218_format_only', auto, 'a Nullable(Int64)'));
-- `union` mode merges the per-file structures before empty names are dropped, so the format-only
-- carve-out has to survive that path too.
SELECT count() FROM (SELECT * FROM file(currentDatabase() || '_05218_format_only', auto, 'a Nullable(Int64)'))
SETTINGS schema_inference_mode = 'union';
-- The extension-less fixture is the only one whose format is detected from the bytes, which is a
-- second cache writer. Nothing it publishes may carry an empty name either.
SELECT count() FROM system.schema_inference_cache
WHERE storage = 'File' AND source LIKE '%_05218_format_only%' AND schema IS NOT NULL;

-- A read that is supposed to be warm has to actually consume the cached structure: otherwise the
-- repeated reads above silently become repeated cold reads and keep matching.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['SchemaInferenceCacheSchemaHits'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%05218 warm default%' AND query NOT LIKE '%ProfileEvents%'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['SchemaInferenceCacheSchemaHits'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%05218 warm union%' AND query NOT LIKE '%ProfileEvents%'
ORDER BY event_time_microseconds DESC LIMIT 1;

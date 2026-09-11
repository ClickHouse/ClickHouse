-- Tags: no-parallel

DROP TABLE IF EXISTS test_03788_bom;
CREATE TABLE test_03788_bom (id UInt8, name String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_03788_bom VALUES (1, 'hello'), (2, 'world');

-- Test CSV with BOM
SELECT * FROM test_03788_bom FORMAT CSV SETTINGS output_format_csv_write_bom = 1;

-- Test CSV without BOM (default)
SELECT * FROM test_03788_bom FORMAT CSV;

-- Test CSVWithNames with BOM
SELECT * FROM test_03788_bom FORMAT CSVWithNames SETTINGS output_format_csv_write_bom = 1;

-- Test TSV with BOM
SELECT * FROM test_03788_bom FORMAT TabSeparated SETTINGS output_format_tsv_write_bom = 1;

-- Test TSV without BOM (default)
SELECT * FROM test_03788_bom FORMAT TabSeparated;

-- Test TSVWithNames with BOM
SELECT * FROM test_03788_bom FORMAT TabSeparatedWithNames SETTINGS output_format_tsv_write_bom = 1;

DROP TABLE test_03788_bom;

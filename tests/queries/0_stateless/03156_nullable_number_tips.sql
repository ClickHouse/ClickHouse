SET output_format_pretty_display_footer_column_names=0;
SELECT 123456789 AS x FORMAT PrettyCompact;
SELECT toNullable(123456789) AS x FORMAT PrettyCompact;
SELECT toLowCardinality(toNullable(123456789)) AS x FORMAT PrettyCompact;
SELECT toNullable(toLowCardinality(123456789)) AS x FORMAT PrettyCompact;
SELECT toLowCardinality(123456789) AS x FORMAT PrettyCompact;

CREATE TEMPORARY TABLE test (x Nullable(UInt64), PRIMARY KEY ()) ENGINE = MergeTree SETTINGS ratio_of_defaults_for_sparse_serialization = 0;
INSERT INTO test SELECT number % 2 ? number * 123456789 : NULL FROM numbers(10);

SELECT DISTINCT dumpColumnStructure(*) FROM test;

SELECT * FROM test ORDER BY ALL DESC NULLS LAST LIMIT 1 FORMAT PRETTY;
SELECT * FROM test ORDER BY ALL ASC NULLS LAST LIMIT 1 FORMAT PRETTY;
SELECT * FROM test ORDER BY ALL ASC NULLS FIRST LIMIT 1 FORMAT PrettySpace;

DROP TEMPORARY TABLE test;
CREATE TEMPORARY TABLE test (x UInt64, PRIMARY KEY ()) ENGINE = MergeTree SETTINGS ratio_of_defaults_for_sparse_serialization = 0;
INSERT INTO test SELECT number % 2 ? number * 123456789 : NULL FROM numbers(10);

SELECT DISTINCT dumpColumnStructure(*) FROM test;

SELECT * FROM test ORDER BY ALL DESC NULLS LAST LIMIT 1 FORMAT PRETTY;
SELECT * FROM test ORDER BY ALL ASC NULLS LAST LIMIT 1 FORMAT PRETTY;
SELECT * FROM test ORDER BY ALL ASC NULLS FIRST LIMIT 1 FORMAT PrettySpace;

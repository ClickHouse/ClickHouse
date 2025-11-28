-- Tags: no-fasttest, no-parallel

INSERT INTO TABLE FUNCTION file('f', 'Parquet', 'c0 Tuple(c1 Map(Int,Int))') SELECT c0 FROM generateRandom('c0 Tuple(c1 Map(Int,Int))', 5638103928316992985, 1, 1) LIMIT 1;
SELECT 1 FROM file('f', 'Parquet', 'c0 Tuple(c1 Map(Int,Int))') tx GROUP BY c0 SETTINGS input_format_parquet_use_native_reader = 1;

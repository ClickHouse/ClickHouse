CREATE TABLE 03755_final_prewhere_duplicate_columns (c0 UInt8) ENGINE = AggregatingMergeTree() ORDER BY (farmHash64(c0));
INSERT INTO TABLE 03755_final_prewhere_duplicate_columns (c0) SELECT 244 FROM numbers(10);
INSERT INTO TABLE 03755_final_prewhere_duplicate_columns (c0) SELECT c0 FROM generateRandom('c0 UInt8', 8343867420892784557, 1, 1) LIMIT 100;
SELECT 03755_final_prewhere_duplicate_columns.c0 FROM 03755_final_prewhere_duplicate_columns FINAL PREWHERE 03755_final_prewhere_duplicate_columns.c0 ORDER BY 03755_final_prewhere_duplicate_columns.c0;
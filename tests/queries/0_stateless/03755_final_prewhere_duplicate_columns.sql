CREATE TABLE 03755_final_prewhere_duplicate_columns (c0 UInt8) ENGINE = AggregatingMergeTree() ORDER BY (c0);
INSERT INTO TABLE 03755_final_prewhere_duplicate_columns (c0) SELECT 2 FROM numbers(3);
INSERT INTO TABLE 03755_final_prewhere_duplicate_columns (c0) SELECT number FROM numbers(10);
SELECT 03755_final_prewhere_duplicate_columns.c0 FROM 03755_final_prewhere_duplicate_columns FINAL PREWHERE 03755_final_prewhere_duplicate_columns.c0 ORDER BY c0;

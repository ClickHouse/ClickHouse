-- Tags: memory-engine
-- NOTE: database = currentDatabase() is not mandatory

-- Pin off: shrinking over-allocated columns on INSERT lowers the bytes the Memory
-- engine accounts for, changing the total_bytes assertion below (8192 -> 8128).
SET shrink_over_allocated_columns_min_waste_ratio = 1.0;

CREATE TEMPORARY TABLE 02271_temporary_table_show_rows_bytes (A Int64) Engine=Memory as SELECT * FROM numbers(1000);
SELECT database, name, total_rows, total_bytes FROM system.tables WHERE is_temporary AND name = '02271_temporary_table_show_rows_bytes';

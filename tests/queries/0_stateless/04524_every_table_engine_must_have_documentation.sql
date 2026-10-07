-- Every table engine must carry a non-empty description in its structured documentation.
SELECT name FROM system.table_engines WHERE length(description) < 10;

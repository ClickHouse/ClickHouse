-- Every table function must carry a non-empty description in its structured documentation.
SELECT name FROM system.table_functions WHERE length(description) < 10;

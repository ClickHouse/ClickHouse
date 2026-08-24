-- Every database engine must carry a non-empty description in its structured documentation.
SELECT name FROM system.database_engines WHERE length(description) < 10;

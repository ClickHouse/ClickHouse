-- Test for querying dangling Alias table via remote()
-- When an Alias table points to a non-existent table, querying via remote() should throw UNKNOWN_TABLE

SET allow_experimental_alias_table_engine = 1;

CREATE TABLE base_table (c1 Int32) ENGINE = TinyLog;

CREATE TABLE alias_table ENGINE = Alias(currentDatabase(), base_table);

-- Drop the base table, leaving the Alias dangling
DROP TABLE base_table;

-- Querying via remote() should throw UNKNOWN_TABLE instead of Logical error
SELECT * FROM remote('localhost:9900', currentDatabase(), alias_table); -- { serverError UNKNOWN_TABLE }

-- Cleanup
DROP TABLE IF EXISTS alias_table;

-- Tags: no-fasttest

-- This outputs the list of undocumented table functions.
-- No new items in the list should appear. Please help shorten this list down to zero elements.
SELECT name
FROM system.table_functions
WHERE length(description) < 10
ORDER BY name;

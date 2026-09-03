-- Test that system.columns has 'column' as an alias for 'name'

-- Query using the original 'name' column
SELECT name FROM system.columns WHERE database = 'system' AND table = 'columns' LIMIT 1;

-- Query using the new 'column' alias - should return identical results  
SELECT column FROM system.columns WHERE database = 'system' AND table = 'columns' LIMIT 1;

-- Test that both column names work in the same query
SELECT name, column, name = column as alias_works FROM system.columns 
WHERE database = 'system' AND table = 'columns' LIMIT 1;
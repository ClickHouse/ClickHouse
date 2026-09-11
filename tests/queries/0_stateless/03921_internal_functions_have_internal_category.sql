-- Internal SQL functions are functions with a `__` prefix.
-- Test that system.functions shows categories = 'Internal' for them
SELECT name, throwIf(categories <> 'Internal')
FROM system.functions
WHERE startsWith(name, '__')
FORMAT Null

-- Tags: no-fasttest

-- This outputs the list of functions without version information.
-- No new items in the list should appear. Please help shorten this list down to zero elements.
SELECT name
FROM system.functions
WHERE
    introduced_in == ''
    AND NOT is_aggregate -- TODO remove this condition
    AND origin = 'System'
    AND alias_to = ''
    AND NOT categories = 'Internal' -- Internal functions are not documented externally
ORDER BY name;

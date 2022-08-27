-- This outputs the list of undocumented functions. No new items in the list should appear.
-- Please help shorten this list down to zero elements.
SELECT name FROM system.functions WHERE NOT is_aggregate AND origin = 'System' AND alias_to = '' AND length(description) < 10 ORDER BY name;

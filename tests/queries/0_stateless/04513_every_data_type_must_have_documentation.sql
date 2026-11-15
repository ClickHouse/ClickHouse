-- Every (non-alias) data type must carry a non-empty description in its structured documentation.
-- Type-name aliases (e.g. BIGINT -> Int64) inherit their target's documentation and are exempt.
SELECT name FROM system.data_type_families WHERE length(description) < 10 AND alias_to = '';

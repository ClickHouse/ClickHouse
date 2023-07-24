SELECT transform(name, ['a', 'b'], ['', NULL]) AS name FROM (SELECT 'test'::Nullable(FixedString(4)) AS name);

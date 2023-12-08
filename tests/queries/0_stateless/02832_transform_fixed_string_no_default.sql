SELECT transform(name, ['a', 'b'], ['', NULL]) AS name FROM (SELECT 'test'::Nullable(FixedString(4)) AS name);
SELECT transform(name, ['test', 'b'], ['', NULL]) AS name FROM (SELECT 'test'::Nullable(FixedString(4)) AS name);
SELECT transform(name, ['a', 'test'], ['', NULL]) AS name FROM (SELECT 'test'::Nullable(FixedString(4)) AS name);

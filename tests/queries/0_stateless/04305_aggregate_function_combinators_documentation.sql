-- Aggregate function combinators expose embedded documentation via system.aggregate_function_combinators.

-- User-facing combinators must have a non-empty description and syntax.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.aggregate_function_combinators
WHERE name IN ('If', 'Array', 'State', 'Merge', 'Distinct')
ORDER BY name;

-- Related combinators are exposed as an array.
SELECT related
FROM system.aggregate_function_combinators
WHERE name = 'State';

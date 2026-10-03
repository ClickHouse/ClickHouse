SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

-- Bare identifiers remain positional. This preserves explicit CAST semantics when
-- the destination tuple uses different names.
SELECT tuple(v)::Tuple(c UInt64) FROM (SELECT toUInt64(5) AS v);
SELECT toTypeName(tuple(v)) FROM (SELECT toUInt64(5) AS v);

-- Explicit aliases opt in to named tuple elements.
SELECT toTypeName(tuple(v AS a)) FROM (SELECT toUInt64(5) AS v);

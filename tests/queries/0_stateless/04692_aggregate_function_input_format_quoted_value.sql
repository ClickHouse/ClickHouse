-- Backward compatibility of `aggregate_function_input_format = 'value'` (released in `v25.12` / `v26.1`) for
-- quoted whole-field payloads: released parsed the field with the argument type's `deserializeTextCSV`, which
-- strips a pair of surrounding CSV quotes. The output below is byte-identical on released `26.7.1`.

SET aggregate_function_input_format = 'value';

-- Scalar types: `readCSVSimple` strips both quote kinds.

SELECT 'UInt64', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, UInt64)', $$'42'$$);
SELECT 'UInt64', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, UInt64)', $$"42"$$);
SELECT 'UInt64', anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, UInt64)', $$'42'$$);
SELECT 'UInt64', anyMerge(x) FROM format(CSV, 'x AggregateFunction(any, UInt64)', $$'42'$$);
SELECT 'Float64', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Float64)', $$'4.5'$$);
SELECT 'Date', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Date)', $$'2020-01-01'$$);
SELECT 'DateTime', anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, DateTime(\'UTC\'))', $$"2020-01-01 10:20:30"$$);
SELECT 'IPv4', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, IPv4)', $$'1.2.3.4'$$);
SELECT 'Decimal', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Decimal(5, 2))', $$'4.25'$$);

-- `Nullable` and `LowCardinality` arguments take the same released parse.

SELECT 'Nullable(UInt64)', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(UInt64))', $$'42'$$);
SELECT 'Nullable(UInt64)', anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, Nullable(UInt64))', $$"42"$$);
SELECT 'Nullable(String)', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(String))', $$"abc"$$);
SELECT 'LowCardinality(String)', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, LowCardinality(String))', $$"abc"$$);

-- The quote kinds are not interchangeable for `String`-like arguments: the CSV string parse knows only `"`,
-- so a single-quoted payload stays a part of the string.

SELECT 'String', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, String)', $$"abc"$$);
SELECT 'String', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, String)', $$'abc'$$);
SELECT 'String', anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, String)', $$"a,b"$$);
SELECT 'FixedString', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, FixedString(5))', $$"abc"$$);
SELECT 'Enum', anyMerge(x) FROM format(TSVRaw, $$x AggregateFunction(any, Enum8('a' = 1, 'b' = 2))$$, $$"b"$$);

-- The self-describing types resolve the unquoted content by its own text, as released did.

SELECT 'Dynamic', toTypeName(anyMerge(x)), anyMerge(x)::String FROM format(TSVRaw, 'x AggregateFunction(any, Dynamic)', $$"42"$$);
SELECT 'Dynamic', anyMerge(x)::String FROM format(TSVRaw, 'x AggregateFunction(any, Dynamic)', $$'42'$$);
SELECT 'Variant', anyMerge(x)::String FROM format(TSVRaw, 'x AggregateFunction(any, Variant(String, UInt64))', $$'42'$$);
SELECT 'Array', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Array(UInt64))', $$"[1,2]"$$);

-- Unquoted payloads keep the unified parse.

SELECT 'unquoted', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, UInt64)', $$42$$);
SELECT 'unquoted', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, String)', $$abc$$);

-- The quoted payload is not narrowed by `input_format_tsv_enum_as_number` either: the released CSV parse never
-- consulted it, so the enum name keeps working.

SET input_format_tsv_enum_as_number = 1;
SELECT 'enum_as_number', anyMerge(x) FROM format(TSVRaw, $$x AggregateFunction(any, Enum8('a' = 1, 'b' = 2))$$, $$"b"$$);
SELECT 'enum_as_number', anyMerge(x) FROM format(TSVRaw, $$x AggregateFunction(any, Enum8('a' = 1, 'b' = 2))$$, $$"2"$$);

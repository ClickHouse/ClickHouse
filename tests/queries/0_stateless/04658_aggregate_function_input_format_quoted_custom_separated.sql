-- The released implementation of `deserializeTextQuoted` always read the whole quoted token as a string and
-- parsed its content, so a quoted payload was accepted by every `Quoted` caller: `CustomSeparated` with
-- `format_custom_escaping_rule = 'Quoted'`, `MySQLDump` and `VALUES`. `VALUES` masks a failure with its
-- expression fallback, the others do not. Check that the quoted forms still work.

SET format_custom_escaping_rule = 'Quoted';

SET aggregate_function_input_format = 'array';
SELECT 'array', avgMerge(x) FROM format(CustomSeparated, 'x AggregateFunction(avg, UInt32)', '\'[1, 2]\'\n');
SELECT 'array group', groupArrayMerge(x) FROM format(CustomSeparated, 'x AggregateFunction(groupArray, UInt32)', '\'[1, 2, 3]\'\n');
SELECT 'array string', groupArrayMerge(x) FROM format(CustomSeparated, 'x AggregateFunction(groupArray, String)', '\'["a", "b"]\'\n');

SET aggregate_function_input_format = 'value';
SELECT 'value', avgMerge(x) FROM format(CustomSeparated, 'x AggregateFunction(avg, UInt32)', '\'42\'\n');
SELECT 'value string', anyMerge(x) FROM format(CustomSeparated, 'x AggregateFunction(any, String)', '\'hello\'\n');
-- The CSV null representation of a single `Nullable` argument keeps working, as released.
SELECT 'value null', anyMerge(x) IS NULL FROM format(CustomSeparated, 'x AggregateFunction(any, Nullable(UInt64))', '\'\\\\N\'\n');

-- The unquoted native forms are unaffected: they do not start with a quote. `Quoted` escaping requires the
-- quote, so exercise them through `TabSeparated` instead.
SELECT 'native value', avgMerge(x) FROM format(TabSeparated, 'x AggregateFunction(avg, UInt32)', '42\n');
SET aggregate_function_input_format = 'array';
SELECT 'native array', avgMerge(x) FROM format(TabSeparated, 'x AggregateFunction(avg, UInt32)', '[1, 2]\n');

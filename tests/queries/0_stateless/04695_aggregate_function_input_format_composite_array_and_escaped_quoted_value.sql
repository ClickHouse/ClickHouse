-- Backward compatibility of `aggregate_function_input_format` for two forms the released implementation
-- (`v25.12`, `v26.1`) accepted through the per-value `deserializeTextCSV` parse:
--   * `array` mode with a composite single argument, where every element was parsed as a CSV field, so the
--     whole composite value could be double-quoted;
--   * `value` mode in the escaped text formats, where the field was decoded before the CSV parse, so the
--     surrounding quotes could themselves be escaped.
-- The native forms added by the unified implementation are checked alongside them.

SET aggregate_function_input_format = 'array';

SELECT '-- array mode, composite argument, released double-quoted element';
SELECT finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Array(UInt64))', '["[1,2]","[3]"]');
SELECT finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Map(String, UInt64))', '["{\'a\':1}","{\'b\':2}"]');
SELECT finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, JSON)', '["{""a"":1}"]');
SELECT finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Tuple(UInt64, String))', '[1,"a"]');

SELECT '-- array mode, composite argument, native element';
SELECT finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Array(UInt64))', '[[1,2],[3]]');
SELECT finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Map(String, UInt64))', '[{\'a\':1},{\'b\':2}]');
SELECT finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Tuple(UInt64, String))', '[(1,\'a\'),(2,\'b\')]');

SET aggregate_function_input_format = 'value';

SELECT '-- value mode, escaped format, escaped surrounding quotes';
SELECT finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, UInt64)', '\\"42\\"');
SELECT finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, UInt64)', '\\\'42\\\'');
SELECT finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, String)', '\\"abc\\"');
SELECT finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, Date)', '\\"2020-01-01\\"');
SELECT finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, Nullable(UInt64))', '\\"42\\"');

SELECT '-- value mode, escaped format, other escape sequences are unaffected';
SELECT finalizeAggregation(x) IS NULL FROM format(TabSeparated, 'x AggregateFunction(any, Nullable(UInt64))', '\\N');
SELECT finalizeAggregation(x) IS NULL FROM format(TabSeparated, 'x AggregateFunction(any, Nullable(String))', '\\N');
SELECT finalizeAggregation(x) = 'a\tb' FROM format(TabSeparated, 'x AggregateFunction(any, String)', 'a\\tb');

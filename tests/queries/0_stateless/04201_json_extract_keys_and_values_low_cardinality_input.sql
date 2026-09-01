-- JSONExtractKeysAndValues accepts a non-constant LowCardinality(String) JSON argument.
SELECT JSONExtractKeysAndValues(materialize(toLowCardinality('{"a": "hello", "b": "world"}')), 'String');

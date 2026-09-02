DROP TABLE IF EXISTS uncomparable_keys;

CREATE TABLE foo (id UInt64, key AggregateFunction(max, UInt64)) ENGINE MergeTree ORDER BY key; --{serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}

CREATE TABLE foo (id UInt64, key AggregateFunction(max, UInt64)) ENGINE MergeTree PARTITION BY key; --{serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}

CREATE TABLE foo (id UInt64, key AggregateFunction(max, UInt64)) ENGINE MergeTree ORDER BY (key) SAMPLE BY key; --{serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY}

DROP TABLE IF EXISTS uncomparable_keys;

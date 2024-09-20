DROP TABLE IF EXISTS uncomparable_keys;

CREATE TABLE foo (id UInt64, key AggregateFunction(max, UInt64)) ENGINE MergeTree ORDER BY key; --{serverError 549}

CREATE TABLE foo (id UInt64, key AggregateFunction(max, UInt64)) ENGINE MergeTree PARTITION BY key; --{serverError 549}

CREATE TABLE foo (id UInt64, key AggregateFunction(max, UInt64)) ENGINE MergeTree ORDER BY (key) SAMPLE BY key; --{serverError 549}

DROP TABLE IF EXISTS uncomparable_keys;

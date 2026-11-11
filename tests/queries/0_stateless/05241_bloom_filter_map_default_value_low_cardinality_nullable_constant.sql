-- `m['k']` returns the default value for rows without the key `k`, so an index on the map cannot be used
-- when the constant equals that default. The default must be taken from the value of the constant, not from
-- its type: the default of `LowCardinality(Nullable(String))` is NULL, while the constant is ''.

DROP TABLE IF EXISTS t_map_keys;
DROP TABLE IF EXISTS t_map_values;
DROP TABLE IF EXISTS t_map_keys_tokenbf;

CREATE TABLE t_map_keys (id UInt64, m Map(String, String), INDEX idx mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

CREATE TABLE t_map_values (id UInt64, m Map(String, String), INDEX idx mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

CREATE TABLE t_map_keys_tokenbf (id UInt64, m Map(String, String), INDEX idx mapKeys(m) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_map_keys SELECT number, if(number % 2 = 0, map('k', 'v'), map('x', 'v')) FROM numbers(10);
INSERT INTO t_map_values SELECT number, if(number % 2 = 0, map('k', 'v'), map('x', 'v')) FROM numbers(10);
INSERT INTO t_map_keys_tokenbf SELECT number, if(number % 2 = 0, map('k', 'v'), map('x', 'v')) FROM numbers(10);

-- { echo }

SELECT count() FROM t_map_keys WHERE m['k'] = '';
SELECT count() FROM t_map_keys WHERE m['k'] = CAST('' AS Nullable(String));
SELECT count() FROM t_map_keys WHERE m['k'] = CAST('' AS LowCardinality(String));
SELECT count() FROM t_map_keys WHERE m['k'] = CAST('' AS LowCardinality(Nullable(String)));
SELECT count() FROM t_map_keys WHERE m['k'] = CAST('' AS LowCardinality(Nullable(String))) SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_map_values WHERE m['k'] = CAST('' AS LowCardinality(Nullable(String)));
SELECT count() FROM t_map_values WHERE m['k'] = CAST('' AS LowCardinality(Nullable(String))) SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_map_keys_tokenbf WHERE m['k'] = CAST('' AS LowCardinality(Nullable(String)));
SELECT count() FROM t_map_keys_tokenbf WHERE m['k'] = CAST('' AS LowCardinality(Nullable(String))) SETTINGS use_skip_indexes = 0;

-- { echoOff }

DROP TABLE t_map_keys;
DROP TABLE t_map_values;
DROP TABLE t_map_keys_tokenbf;

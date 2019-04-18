SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.delta_codec_synthetic;
DROP TABLE IF EXISTS test.default_codec_synthetic;

CREATE TABLE test.delta_codec_synthetic
(
    id UInt64 Codec(Delta, ZSTD)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE TABLE test.default_codec_synthetic
(
    id UInt64 Codec(ZSTD)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192;

INSERT INTO test.delta_codec_synthetic SELECT number FROM system.numbers LIMIT 5000000;
INSERT INTO test.default_codec_synthetic SELECT number FROM system.numbers LIMIT 5000000;

OPTIMIZE TABLE test.delta_codec_synthetic FINAL;
OPTIMIZE TABLE test.default_codec_synthetic FINAL;

SELECT
    floor(big_size / small_size) AS ratio
FROM
    (SELECT 1 AS key, sum(bytes_on_disk) AS small_size FROM system.parts WHERE database == 'test' and table == 'delta_codec_synthetic')
INNER JOIN
    (SELECT 1 AS key, sum(bytes_on_disk) as big_size FROM system.parts WHERE database == 'test' and table == 'default_codec_synthetic')
USING(key);

SELECT
    small_hash == big_hash
FROM
    (SELECT 1 AS key, sum(cityHash64(*)) AS small_hash FROM test.delta_codec_synthetic)
INNER JOIN
    (SELECT 1 AS key, sum(cityHash64(*)) AS big_hash FROM test.default_codec_synthetic)
USING(key);

DROP TABLE IF EXISTS test.delta_codec_synthetic;
DROP TABLE IF EXISTS test.default_codec_synthetic;

DROP TABLE IF EXISTS test.delta_codec_float;
DROP TABLE IF EXISTS test.default_codec_float;

CREATE TABLE test.delta_codec_float
(
    id Float64 Codec(Delta, LZ4HC)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE TABLE test.default_codec_float
(
    id Float64 Codec(LZ4HC)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192;

INSERT INTO test.delta_codec_float SELECT number FROM numbers(1547510400, 500000) WHERE number % 3 == 0 OR number % 5 == 0 OR number % 7 == 0 OR number % 11 == 0;
INSERT INTO test.default_codec_float SELECT * from test.delta_codec_float;

OPTIMIZE TABLE test.delta_codec_float FINAL;
OPTIMIZE TABLE test.default_codec_float FINAL;

SELECT
    floor(big_size / small_size) as ratio
FROM
    (SELECT 1 AS key, sum(bytes_on_disk) AS small_size FROM system.parts WHERE database = 'test' and table = 'delta_codec_float')
INNER JOIN
    (SELECT 1 AS key, sum(bytes_on_disk) as big_size FROM system.parts WHERE database = 'test' and table = 'default_codec_float') USING(key);

SELECT
    small_hash == big_hash
FROM
    (SELECT 1 AS key, sum(cityHash64(*)) AS small_hash FROM test.delta_codec_float)
INNER JOIN
    (SELECT 1 AS key, sum(cityHash64(*)) AS big_hash FROM test.default_codec_float)
USING(key);

DROP TABLE IF EXISTS test.delta_codec_float;
DROP TABLE IF EXISTS test.default_codec_float;


DROP TABLE IF EXISTS test.delta_codec_string;
DROP TABLE IF EXISTS test.default_codec_string;

CREATE TABLE test.delta_codec_string
(
    id Float64 Codec(Delta, LZ4)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE TABLE test.default_codec_string
(
    id Float64 Codec(LZ4)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192;

INSERT INTO test.delta_codec_string SELECT concat(toString(number), toString(number % 100)) FROM numbers(1547510400, 500000);
INSERT INTO test.default_codec_string SELECT * from test.delta_codec_string;

OPTIMIZE TABLE test.delta_codec_string FINAL;
OPTIMIZE TABLE test.default_codec_string FINAL;

SELECT
    floor(big_size / small_size) as ratio
FROM
    (SELECT 1 AS key, sum(bytes_on_disk) AS small_size FROM system.parts WHERE database = 'test' and table = 'delta_codec_string')
INNER JOIN
    (SELECT 1 AS key, sum(bytes_on_disk) as big_size FROM system.parts WHERE database = 'test' and table = 'default_codec_string') USING(key);

SELECT
    small_hash == big_hash
FROM
    (SELECT 1 AS key, sum(cityHash64(*)) AS small_hash FROM test.delta_codec_string)
INNER JOIN
    (SELECT 1 AS key, sum(cityHash64(*)) AS big_hash FROM test.default_codec_string)
USING(key);

DROP TABLE IF EXISTS test.delta_codec_string;
DROP TABLE IF EXISTS test.default_codec_string;

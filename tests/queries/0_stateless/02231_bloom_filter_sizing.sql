SELECT 'Bloom filter on sort key';
DROP TABLE IF EXISTS bloom_filter_sizing_pk;
CREATE TABLE bloom_filter_sizing_pk(
  key UInt64,
  value UInt64,

  -- Very high granularity to have one filter per part.
  INDEX key_bf key TYPE bloom_filter(0.01) GRANULARITY 2147483648
) ENGINE=MergeTree ORDER BY key;

INSERT INTO bloom_filter_sizing_pk
SELECT
number % 100 as key, -- 100 unique keys
number as value -- whatever
FROM numbers(100_000);

--
-- Merge everything into a single part
--
OPTIMIZE TABLE bloom_filter_sizing_pk FINAL;

SELECT COUNT() from bloom_filter_sizing_pk WHERE key = 1;

-- Check bloom filter size. According to https://hur.st/bloomfilter/?n=100&p=0.01 for 100 keys it should be less that 200B
SELECT COUNT() from system.parts where database = currentDatabase() AND table = 'bloom_filter_sizing_pk' and secondary_indices_uncompressed_bytes > 200 and active;

SELECT 'Bloom filter on non-sort key';
DROP TABLE IF EXISTS bloom_filter_sizing_sec;
CREATE TABLE bloom_filter_sizing_sec(
  key1 UInt64,
  key2 UInt64,
  value UInt64,

  -- Very high granularity to have one filter per part.
  INDEX key_bf key2 TYPE bloom_filter(0.01) GRANULARITY 2147483648
) ENGINE=MergeTree ORDER BY key1;

INSERT INTO bloom_filter_sizing_sec
SELECT
number % 100 as key1, -- 100 unique keys
rand() % 100 as key2, -- 100 unique keys
number as value -- whatever
FROM numbers(100_000);

--
-- Merge everything into a single part
--
OPTIMIZE TABLE bloom_filter_sizing_sec FINAL;

SELECT COUNT() from bloom_filter_sizing_sec WHERE key1 = 1;

-- Check bloom filter size. According to https://hur.st/bloomfilter/?n=100&p=0.01 for 100 keys it should be less that 200B
SELECT COUNT() from system.parts where database = currentDatabase() AND table = 'bloom_filter_sizing_sec' and secondary_indices_uncompressed_bytes > 200 and active;

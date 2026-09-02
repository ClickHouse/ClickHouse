-- Tags: shard

-- With the old analyzer, `StorageDistributed` prunes shards by folding constant expressions into untyped
-- literals (`replaceConstantExpressions`). A `UUID`-family constant is re-encoded as text there, because an
-- untyped literal cannot tell `UUID` from `UUID2` apart. That text form is itself ambiguous against a
-- `Variant`/`Dynamic`/`JSON` column: `convertFieldToType` accepts a string into the `String` alternative, so
-- the sharding expression would be evaluated over a string rather than over the `UUID2` alternative and could
-- prune away the shard that owns the matching row. Such tables must not be pruned at all.

SET enable_analyzer = 0;
SET optimize_skip_unused_shards = 1;
SET prefer_localhost_replica = 0;
SET allow_experimental_variant_type = 1;

DROP TABLE IF EXISTS data_variant_05056;
DROP TABLE IF EXISTS data_uuid2_05056;

CREATE TABLE data_variant_05056 (v Variant(String, UUID2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_variant_05056 VALUES ('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'::UUID2), ('e0b18ec7-5971-44e6-a463-eea17c37a532'::UUID2);

-- Both "shards" of `remote('127.{1,2}', ...)` are the same local table, so the matching row is returned once
-- per queried shard: `queried_shards` is exactly the number of shards that survived pruning. A `Variant`
-- column must keep both of them, so that the row can never be lost.

SELECT 'Variant(String, UUID2) sharding key, not pruned';
SELECT uniqExact(_shard_num) AS queried_shards, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_variant_05056, sipHash64(v))
WHERE v = '4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'::UUID2;

SELECT uniqExact(_shard_num) AS queried_shards, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_variant_05056, sipHash64(v))
WHERE v = 'e0b18ec7-5971-44e6-a463-eea17c37a532'::UUID2;

-- A plain `UUID2` column is unambiguous, so pruning still applies there: exactly one shard is queried.

CREATE TABLE data_uuid2_05056 (id UUID2) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_uuid2_05056 VALUES ('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'), ('e0b18ec7-5971-44e6-a463-eea17c37a532');

SELECT 'UUID2 sharding key, still pruned';
SELECT uniqExact(_shard_num) AS queried_shards, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid2_05056, sipHash64(id))
WHERE id = '4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'::UUID2;

DROP TABLE data_variant_05056;
DROP TABLE data_uuid2_05056;

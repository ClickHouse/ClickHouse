-- Tags: shard

-- Shard pruning must interpret an `IN` constant in the layout of the sharding key column.
-- `UUID` and `UUID2` share the `Field` representation but swap the two 64-bit halves, so a
-- `toUUID(...)` constant compared with a `UUID2` sharding key (and the other way around) used to be
-- fed to the sharding expression as the wrong 16 bytes, and `optimize_skip_unused_shards_rewrite_in`
-- could drop the value from the shard that owns the matching row.

DROP TABLE IF EXISTS data_uuid2_04692;
DROP TABLE IF EXISTS data_uuid1_04692;

CREATE TABLE data_uuid2_04692 (id UUID2) ENGINE = MergeTree ORDER BY id;
CREATE TABLE data_uuid1_04692 (id UUID) ENGINE = MergeTree ORDER BY id;

INSERT INTO data_uuid2_04692 VALUES ('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'), ('df195aeb-b02c-43ef-a626-91144d58eee5'), ('348b6260-5dfc-4b49-ae88-7aa235e9a28d'), ('28feb35b-17cd-4d01-b6a8-22b734752e44'), ('e0b18ec7-5971-44e6-a463-eea17c37a532'), ('dec6a463-639e-49df-b821-50d68c0e8dc1'), ('f22b6913-7d49-4760-8856-3b9307bda8b6'), ('706e398e-b00c-42f8-aec8-db115aaa77b9');
INSERT INTO data_uuid1_04692 SELECT id::UUID FROM data_uuid2_04692;

SET optimize_skip_unused_shards = 1;
SET optimize_skip_unused_shards_rewrite_in = 1;
SET prefer_localhost_replica = 0;
-- The typed `IN` rewrite exists only in the analyzer plan; with the old analyzer the `toUUID(...)`
-- constants stay unresolved function calls, nothing is pruned, and every value matches on both shards.
SET enable_analyzer = 1;

-- Both shards hold every row, so a value from the `IN` list survives on exactly one shard - the one the
-- pruning decided owns it. That shard must be the one the sharding expression maps the stored row to.
-- With the wrong layout the constant hashes to the other shard for about half of the values, so the
-- values below are deliberately not symmetric under a half swap.

SELECT 'UUID2 key, UUID constants';
SELECT countIf(_shard_num != (sipHash64(id) % 2) + 1) AS misrouted, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid2_04692, sipHash64(id))
WHERE id IN (
    toUUID('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'),
    toUUID('df195aeb-b02c-43ef-a626-91144d58eee5'),
    toUUID('348b6260-5dfc-4b49-ae88-7aa235e9a28d'),
    toUUID('28feb35b-17cd-4d01-b6a8-22b734752e44'),
    toUUID('e0b18ec7-5971-44e6-a463-eea17c37a532'),
    toUUID('dec6a463-639e-49df-b821-50d68c0e8dc1'),
    toUUID('f22b6913-7d49-4760-8856-3b9307bda8b6'),
    toUUID('706e398e-b00c-42f8-aec8-db115aaa77b9'));

SELECT 'UUID key, UUID2 constants';
SELECT countIf(_shard_num != (sipHash64(id) % 2) + 1) AS misrouted, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid1_04692, sipHash64(id))
WHERE id IN (
    toUUID2('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'),
    toUUID2('df195aeb-b02c-43ef-a626-91144d58eee5'),
    toUUID2('348b6260-5dfc-4b49-ae88-7aa235e9a28d'),
    toUUID2('28feb35b-17cd-4d01-b6a8-22b734752e44'),
    toUUID2('e0b18ec7-5971-44e6-a463-eea17c37a532'),
    toUUID2('dec6a463-639e-49df-b821-50d68c0e8dc1'),
    toUUID2('f22b6913-7d49-4760-8856-3b9307bda8b6'),
    toUUID2('706e398e-b00c-42f8-aec8-db115aaa77b9'));

-- A `UUID2` constant is also written into the query sent to the shard, where it is formatted with `UUID`
-- semantics unless the constant keeps its type: both shards must still find the row.
SELECT 'UUID2 constant sent to the shard';
SELECT count() FROM remote('127.{1,2}', currentDatabase(), data_uuid2_04692)
WHERE id = toUUID2('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29')
SETTINGS optimize_skip_unused_shards = 0, optimize_skip_unused_shards_rewrite_in = 0;

DROP TABLE data_uuid2_04692;
DROP TABLE data_uuid1_04692;

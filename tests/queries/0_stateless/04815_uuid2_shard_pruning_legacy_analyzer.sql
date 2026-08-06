-- Tags: shard

-- With the old analyzer, `StorageDistributed` prunes shards by folding constant expressions into
-- untyped literals (`replaceConstantExpressions`) and evaluating the sharding expression over them
-- (`evaluateExpressionOverConstantCondition`). An untyped literal cannot tell `UUID` from `UUID2`
-- apart (they share the `Field` representation but swap the two 64-bit halves), so a `toUUID(...)`
-- constant compared with a `UUID2` sharding key (and the other way around) used to be hashed as the
-- wrong 16 bytes, and the shard that owns the matching row could be pruned.

SET enable_analyzer = 0;
SET optimize_skip_unused_shards = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS data_uuid2_04815;
DROP TABLE IF EXISTS data_uuid1_04815;

CREATE TABLE data_uuid2_04815 (id UUID2) ENGINE = MergeTree ORDER BY id;
CREATE TABLE data_uuid1_04815 (id UUID) ENGINE = MergeTree ORDER BY id;

INSERT INTO data_uuid2_04815 VALUES ('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'), ('df195aeb-b02c-43ef-a626-91144d58eee5'), ('348b6260-5dfc-4b49-ae88-7aa235e9a28d'), ('28feb35b-17cd-4d01-b6a8-22b734752e44'), ('e0b18ec7-5971-44e6-a463-eea17c37a532'), ('dec6a463-639e-49df-b821-50d68c0e8dc1'), ('f22b6913-7d49-4760-8856-3b9307bda8b6'), ('706e398e-b00c-42f8-aec8-db115aaa77b9');
INSERT INTO data_uuid1_04815 SELECT id::UUID FROM data_uuid2_04815;

-- Both "shards" of `remote('127.{1,2}', ...)` are the same local table, so a row matches on every
-- queried shard. `misrouted = 0, matched = 1` therefore proves that exactly one shard was queried and
-- that it is the shard the sharding expression maps the stored row to. The values are picked so that
-- hashing the wrong 16 bytes maps them to the other shard, which used to make the query miss the row
-- entirely on a real cluster (and shows up here as `misrouted = 1`).

SELECT 'UUID2 key, UUID constant, equality';
SELECT countIf(_shard_num != (sipHash64(id) % 2) + 1) AS misrouted, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid2_04815, sipHash64(id))
WHERE id = toUUID('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29');
SELECT countIf(_shard_num != (sipHash64(id) % 2) + 1) AS misrouted, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid2_04815, sipHash64(id))
WHERE id = toUUID('e0b18ec7-5971-44e6-a463-eea17c37a532');

SELECT 'UUID key, UUID2 constant, equality';
SELECT countIf(_shard_num != (sipHash64(id) % 2) + 1) AS misrouted, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid1_04815, sipHash64(id))
WHERE id = toUUID2('f22b6913-7d49-4760-8856-3b9307bda8b6');
SELECT countIf(_shard_num != (sipHash64(id) % 2) + 1) AS misrouted, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid1_04815, sipHash64(id))
WHERE id = toUUID2('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29');

-- With the old analyzer, the right-hand side of `IN` becomes a prepared set: its constants are never
-- folded into literals, `evaluateExpressionOverConstantCondition` cannot analyze the bare `toUUID(...)`
-- calls, and no shard is pruned. That is safe (no row can be lost), and both mirrors of the "cluster"
-- are queried, so every value matches twice. If pruning is ever taught to handle typed `IN` constants,
-- this changes to `1  2` - and the layout handling above must be revisited for that path.

SELECT 'UUID2 key, UUID constants, IN';
SELECT uniqExact(_shard_num) AS shards_queried, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid2_04815, sipHash64(id))
WHERE id IN (toUUID('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'), toUUID('e0b18ec7-5971-44e6-a463-eea17c37a532'));

SELECT 'UUID key, UUID2 constants, IN';
SELECT uniqExact(_shard_num) AS shards_queried, count() AS matched
FROM remote('127.{1,2}', currentDatabase(), data_uuid1_04815, sipHash64(id))
WHERE id IN (toUUID2('f22b6913-7d49-4760-8856-3b9307bda8b6'), toUUID2('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'));

DROP TABLE data_uuid2_04815;
DROP TABLE data_uuid1_04815;

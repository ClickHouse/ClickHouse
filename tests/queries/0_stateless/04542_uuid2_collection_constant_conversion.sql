-- Constants of collection types (Array, Tuple, Map) built from UUID expressions must convert
-- element-wise into the corresponding UUID2 collection types, swapping the halves of every element.
-- Previously the source-type hint was not propagated into the collection recursion of convertFieldToType,
-- so the elements kept the UUID in-memory layout and were stored or hashed as different UUID2 values.

-- Element-wise conversion in VALUES: without the fix all three inserted values come out scrambled.
DROP TABLE IF EXISTS t_uuid2_collections;
CREATE TABLE t_uuid2_collections (arr Array(UUID2), tup Tuple(UUID2, UInt64), m Map(UUID2, UInt64)) ENGINE = Memory;
INSERT INTO t_uuid2_collections VALUES ([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 42), map(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 42));
SELECT arr, tup, m FROM t_uuid2_collections;
DROP TABLE t_uuid2_collections;

-- has/indexOf/countEqual/mapContains must reconcile the layouts when the haystack elements
-- and the needle are a mix of UUID and UUID2 (previously the raw representations were compared,
-- so logically equal values never matched).
SELECT has([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')),
       has([toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')),
       has([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba1')),
       has([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], materialize(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))),
       has([materialize(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))], materialize(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))),
       indexOf([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')),
       countEqual([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')),
       has([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0')::Nullable(UUID2)),
       mapContains(map(toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 1), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));

-- Constant arrays in bloom filter index analysis: hashing an unswapped element used to
-- produce a false negative (the granule with the matching row was incorrectly pruned).
DROP TABLE IF EXISTS t_uuid2_bf;
CREATE TABLE t_uuid2_bf
(
    id UInt64,
    u UUID2,
    arr Array(UUID2),
    INDEX bf_u u TYPE bloom_filter GRANULARITY 1,
    INDEX bf_arr arr TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_uuid2_bf SELECT
    number,
    toUUID2(concat('61f0c404-5cb3-11e7-907b-a6006ad3db0', toString(number))),
    [toUUID2(concat('61f0c404-5cb3-11e7-907b-a6006ad3db0', toString(number)))]
FROM numbers(4);

SET force_data_skipping_indices = 'bf_arr';
SELECT id FROM t_uuid2_bf WHERE hasAny(arr, [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db02')]);
SELECT id FROM t_uuid2_bf WHERE hasAll(arr, [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db01')]);

SET force_data_skipping_indices = 'bf_u';
SELECT id FROM t_uuid2_bf WHERE has([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db03'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db00')], u) ORDER BY id;

DROP TABLE t_uuid2_bf;

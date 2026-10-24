-- Map bloom-filter key/value predicates on `Map(UUID2, ...)` / `Map(..., UUID2)` must convert the
-- comparison constant to the map's actual key/value type before hashing it against the bloom filter
-- (the `arrayElement(map, key) = value`/`IN` form and the `map.key_<serialized>` subcolumn form),
-- the same way this PR already does for array constants. Otherwise a constant of the "other" UUID
-- flavor is hashed with the wrong in-memory layout and the granule containing the matching row is
-- incorrectly pruned away.

DROP TABLE IF EXISTS t_uuid2_map_bf;
CREATE TABLE t_uuid2_map_bf
(
    id UInt64,
    mk Map(UUID2, UInt64),
    mv Map(UInt64, UUID2),
    INDEX bf_keys mapKeys(mk) TYPE bloom_filter GRANULARITY 1,
    INDEX bf_values mapValues(mv) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_uuid2_map_bf SELECT
    number,
    map(toUUID2(concat('61f0c404-5cb3-11e7-907b-a6006ad3db0', toString(number))), number),
    map(number, toUUID2(concat('61f0c404-5cb3-11e7-907b-a6006ad3db0', toString(number))))
FROM numbers(4);

SET force_data_skipping_indices = 'bf_keys';

SELECT '-- mapKeys(Map(UUID2, UInt64)) bloom filter, lookup key given as UUID';
SELECT id FROM t_uuid2_map_bf WHERE mk[toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db02')] = 2 ORDER BY id;
SELECT id FROM t_uuid2_map_bf WHERE mk[toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db01')] IN (1, 100) ORDER BY id;
SELECT id FROM t_uuid2_map_bf WHERE mapContains(mk, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db03')) ORDER BY id;

SET force_data_skipping_indices = 'bf_values';

SELECT '-- mapValues(Map(UInt64, UUID2)) bloom filter, comparison value given as UUID';
SELECT id FROM t_uuid2_map_bf WHERE mv[2] = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db02') ORDER BY id;
SELECT id FROM t_uuid2_map_bf WHERE mv[1] IN (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db01'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1')) ORDER BY id;
SELECT id FROM t_uuid2_map_bf WHERE mapContainsValue(mv, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3db03')) ORDER BY id;

DROP TABLE t_uuid2_map_bf;

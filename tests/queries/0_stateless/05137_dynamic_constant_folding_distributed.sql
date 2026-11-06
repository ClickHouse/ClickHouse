-- A `Dynamic` constant sent to a secondary server keeps the type of its active member. The member's
-- literal alone does not carry it: an `Enum8` value is written as its number.

-- `prefer_localhost_replica` = 0 sends the `remote` query to the server instead of planning it locally,
-- `serialize_query_plan` = 0 keeps the constant in the query text, and `enable_parallel_replicas` = 0
-- pins the runner's randomization for the `remote` cells.
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_date;
CREATE TABLE t_str (v String) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_str VALUES ('7'), ('3'), ('zz');
CREATE TABLE t_date (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_date VALUES ('2020-01-02'), ('2020-01-03');

-- `Enum8('7' = 3)` has both its name and its number stored in `t_str`, so the wrong member type
-- returns the wrong row rather than no row.
SELECT arraySort(groupArray(v)) FROM remote('127.0.0.1', currentDatabase(), t_str)
WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic);

SELECT arraySort(groupArray(v)) FROM remote('127.0.0.1', currentDatabase(), t_str)
WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic));

-- `Dynamic(max_types = 0)` keeps every value in its shared variant.
SELECT arraySort(groupArray(v)) FROM remote('127.0.0.1', currentDatabase(), t_str)
WHERE v IN (CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic(max_types = 0)));

-- `materialize` makes the secondary server evaluate `dynamicType` on the value it received.
SELECT dynamicType(materialize(CAST(toUInt64(42), 'Dynamic'))), dynamicElement(materialize(CAST(toUInt64(42), 'Dynamic')), 'UInt64')
FROM remote('127.0.0.1', system.one);
SELECT dynamicType(materialize(CAST(toIPv4('1.2.3.4'), 'Dynamic'))), dynamicElement(materialize(CAST(toIPv4('1.2.3.4'), 'Dynamic')), 'IPv4')
FROM remote('127.0.0.1', system.one);
SELECT dynamicType(materialize(CAST(CAST(map('a', toUInt64(1)), 'Map(String, UInt64)'), 'Dynamic')))
FROM remote('127.0.0.1', system.one);

-- Elements of different member types in one `array` need a common type.
SELECT DISTINCT arrayMap(x -> dynamicType(x), materialize([1::Int64::Dynamic, 1::UInt64::Dynamic]))
FROM remote('127.0.0.1', system.one) SETTINGS use_variant_as_common_type = 0;

-- 1698543000 is the second of two instants with the same local time, at the end of daylight saving time.
SELECT dynamicType(c), toUnixTimestamp(c::DateTime('Europe/Berlin'))
FROM (SELECT materialize(CAST(toDateTime(1698543000, 'Europe/Berlin') AS Dynamic)) AS c FROM remote('127.0.0.1', system.one));

-- The same instant held in the shared variant.
SELECT dynamicType(c), toUnixTimestamp(c::DateTime('Europe/Berlin'))
FROM (SELECT materialize(CAST(toDateTime(1698543000, 'Europe/Berlin') AS Dynamic(max_types = 0))) AS c FROM remote('127.0.0.1', system.one));

SELECT arraySort(groupArray(d)) FROM remote('127.0.0.1', currentDatabase(), t_date)
WHERE d IN (CAST(toDateTime('2020-01-02 05:00:00', 'UTC') AS Dynamic));

-- The same constant read through parallel replicas.
SELECT arraySort(groupArray(v)) FROM t_str
WHERE v = CAST(CAST('7', 'Enum8(\'7\' = 3)') AS Dynamic)
SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

DROP TABLE t_str;
DROP TABLE t_date;

-- 03212_variant_dynamic_cast_or_default_repro.sql
SET allow_experimental_dynamic_type = 1;

----------------------------------------------------------------
-- combo #1: dynamic=v2, shared-data=map
----------------------------------------------------------------
DROP TABLE IF EXISTS t_shared_v2_map;
CREATE TABLE t_shared_v2_map
(
    id UInt32,
    d  Dynamic(max_types = 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    dynamic_serialization_version = 'v2',
    object_shared_data_serialization_version = 'map',
    object_shared_data_serialization_version_for_zero_level_parts = 'map';

INSERT INTO t_shared_v2_map SELECT number, number::Int64 FROM numbers(512);
INSERT INTO t_shared_v2_map SELECT 10000 + number, concat('s_', toString(number)) FROM numbers(512);
INSERT INTO t_shared_v2_map SELECT 20000 + number, ( [number] )::Array(Int32) FROM numbers(64);
INSERT INTO t_shared_v2_map VALUES (900000000, toIPv4('192.168.0.1'));
INSERT INTO t_shared_v2_map VALUES (900000001, toIPv6('::1'));

OPTIMIZE TABLE t_shared_v2_map FINAL;

SELECT * FROM
(
    SELECT
        10 AS seq,
        'v2/map ip_rows_in_shared == 2' AS tag,
        toUInt8(
            (SELECT sum(isDynamicElementInSharedData(d)) FROM t_shared_v2_map WHERE id IN (900000000,900000001)) = 2
        ) AS val
    UNION ALL
    SELECT
        11 AS seq,
        'v2/map ipv4_set_ok' AS tag,
        toUInt8(
            (SELECT arraySort(arrayDistinct(groupArray(toString(toIPv4OrDefault(d))))) FROM t_shared_v2_map)
            = ['0.0.0.0','192.168.0.1']
        ) AS val
    UNION ALL
    SELECT
        12 AS seq,
        'v2/map ipv6_set_ok' AS tag,
        toUInt8(
            (SELECT arraySort(arrayDistinct(groupArray(toString(toIPv6OrDefault(d))))) FROM t_shared_v2_map)
            = ['::','::1','::ffff:192.168.0.1']
        ) AS val
    UNION ALL
    SELECT
        13 AS seq,
        'v2/map non_ip_to_ipv4_unexpected_count' AS tag,
        (SELECT countIf(dynamicType(d) NOT IN ('IPv4','String') AND toIPv4OrDefault(d) != toIPv4('0.0.0.0')) FROM t_shared_v2_map) AS val
    UNION ALL
    SELECT
        14 AS seq,
        'v2/map non_ip_to_ipv6_unexpected_count' AS tag,
        (SELECT countIf(dynamicType(d) NOT IN ('IPv6','IPv4','String') AND toIPv6OrDefault(d) != toIPv6('::')) FROM t_shared_v2_map) AS val
) ORDER BY seq;

----------------------------------------------------------------
-- combo #2: dynamic=v3, shared-data=advanced
----------------------------------------------------------------
DROP TABLE IF EXISTS t_shared_v3_adv;
CREATE TABLE t_shared_v3_adv
(
    id UInt32,
    d  Dynamic(max_types = 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    dynamic_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced';

INSERT INTO t_shared_v3_adv SELECT number, number::Int64 FROM numbers(512);
INSERT INTO t_shared_v3_adv SELECT 10000 + number, concat('s_', toString(number)) FROM numbers(512);
INSERT INTO t_shared_v3_adv SELECT 20000 + number, ( [number] )::Array(Int32) FROM numbers(64);
INSERT INTO t_shared_v3_adv VALUES (900000000, toIPv4('192.168.0.1'));
INSERT INTO t_shared_v3_adv VALUES (900000001, toIPv6('::1'));

OPTIMIZE TABLE t_shared_v3_adv FINAL;

SELECT * FROM
(
    SELECT
        20 AS seq,
        'v3/advanced ip_rows_in_shared == 2' AS tag,
        toUInt8(
            (SELECT sum(isDynamicElementInSharedData(d)) FROM t_shared_v3_adv WHERE id IN (900000000,900000001)) = 2
        ) AS val
    UNION ALL
    SELECT
        21 AS seq,
        'v3/advanced ipv4_set_ok' AS tag,
        toUInt8(
            (SELECT arraySort(arrayDistinct(groupArray(toString(toIPv4OrDefault(d))))) FROM t_shared_v3_adv)
            = ['0.0.0.0','192.168.0.1']
        ) AS val
    UNION ALL
    SELECT
        22 AS seq,
        'v3/advanced ipv6_set_ok' AS tag,
        toUInt8(
            (SELECT arraySort(arrayDistinct(groupArray(toString(toIPv6OrDefault(d))))) FROM t_shared_v3_adv)
            = ['::','::1','::ffff:192.168.0.1']
        ) AS val
    UNION ALL
    SELECT
        23 AS seq,
        'v3/advanced non_ip_to_ipv4_unexpected_count' AS tag,
        (SELECT countIf(dynamicType(d) NOT IN ('IPv4','String') AND toIPv4OrDefault(d) != toIPv4('0.0.0.0')) FROM t_shared_v3_adv) AS val
    UNION ALL
    SELECT
        24 AS seq,
        'v3/advanced non_ip_to_ipv6_unexpected_count' AS tag,
        (SELECT countIf(dynamicType(d) NOT IN ('IPv6','IPv4','String') AND toIPv6OrDefault(d) != toIPv6('::')) FROM t_shared_v3_adv) AS val
) ORDER BY seq;

----------------------------------------------------------------
-- combo #3: dynamic=v3, shared-data=map_with_buckets
----------------------------------------------------------------
DROP TABLE IF EXISTS t_shared_v3_buckets;
CREATE TABLE t_shared_v3_buckets
(
    id UInt32,
    d  Dynamic(max_types = 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    dynamic_serialization_version = 'v3',
    object_shared_data_serialization_version = 'map_with_buckets',
    object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets',
    object_shared_data_buckets_for_compact_part = 16,
    object_shared_data_buckets_for_wide_part = 4;

INSERT INTO t_shared_v3_buckets SELECT number, number::Int64 FROM numbers(512);
INSERT INTO t_shared_v3_buckets SELECT 10000 + number, concat('s_', toString(number)) FROM numbers(512);
INSERT INTO t_shared_v3_buckets SELECT 20000 + number, ( [number] )::Array(Int32) FROM numbers(64);
INSERT INTO t_shared_v3_buckets VALUES (900000000, toIPv4('192.168.0.1'));
INSERT INTO t_shared_v3_buckets VALUES (900000001, toIPv6('::1'));

OPTIMIZE TABLE t_shared_v3_buckets FINAL;

SELECT * FROM
(
    SELECT
        30 AS seq,
        'v3/buckets ip_rows_in_shared == 2' AS tag,
        toUInt8(
            (SELECT sum(isDynamicElementInSharedData(d)) FROM t_shared_v3_buckets WHERE id IN (900000000,900000001)) = 2
        ) AS val
    UNION ALL
    SELECT
        31 AS seq,
        'v3/buckets ipv4_set_ok' AS tag,
        toUInt8(
            (SELECT arraySort(arrayDistinct(groupArray(toString(toIPv4OrDefault(d))))) FROM t_shared_v3_buckets)
            = ['0.0.0.0','192.168.0.1']
        ) AS val
    UNION ALL
    SELECT
        32 AS seq,
        'v3/buckets ipv6_set_ok' AS tag,
        toUInt8(
            (SELECT arraySort(arrayDistinct(groupArray(toString(toIPv6OrDefault(d))))) FROM t_shared_v3_buckets)
            = ['::','::1','::ffff:192.168.0.1']
        ) AS val
    UNION ALL
    SELECT
        33 AS seq,
        'v3/buckets non_ip_to_ipv4_unexpected_count' AS tag,
        (SELECT countIf(dynamicType(d) NOT IN ('IPv4','String') AND toIPv4OrDefault(d) != toIPv4('0.0.0.0')) FROM t_shared_v3_buckets) AS val
    UNION ALL
    SELECT
        34 AS seq,
        'v3/buckets non_ip_to_ipv6_unexpected_count' AS tag,
        (SELECT countIf(dynamicType(d) NOT IN ('IPv6','IPv4','String') AND toIPv6OrDefault(d) != toIPv6('::')) FROM t_shared_v3_buckets) AS val
) ORDER BY seq;

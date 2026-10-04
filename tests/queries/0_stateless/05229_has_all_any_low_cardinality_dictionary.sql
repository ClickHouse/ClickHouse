-- Every predicate is evaluated on an Array(LowCardinality(T)) column and on a logically identical
-- Array(T) twin held in the same row, so the two answers must agree row by row as well as in total.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS tags;
CREATE TABLE tags (id UInt64, lc Array(LowCardinality(String)), plain Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tags
SELECT number, arr, arr
FROM
(
    SELECT
        number,
        arrayConcat(
            arrayMap(j -> concat('f', toString(cityHash64(number, j) % 300)), range(3)),
            if(number % 2 = 0, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'], []),
            if(number % 3 = 0, ['c0'], []),
            if(number % 5 = 0, ['c1', 'c2'], []),
            if(number % 7 = 0, [''], [])) AS arr
    FROM numbers(1000)
);

SELECT 'rows', count() FROM tags;
SELECT 'distinct dictionary values', uniqExact(arrayJoin(lc)) FROM tags;

SELECT '1 needle', countIf(hasAll(lc, ['c0'])), countIf(hasAll(plain, ['c0'])),
    sum(cityHash64(id, hasAll(lc, ['c0']))) = sum(cityHash64(id, hasAll(plain, ['c0']))) FROM tags;
SELECT '8 needles', countIf(hasAll(lc, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'])), countIf(hasAll(plain, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'])),
    sum(cityHash64(id, hasAll(lc, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7']))) = sum(cityHash64(id, hasAll(plain, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7']))) FROM tags;
SELECT 'mixed needles', countIf(hasAll(lc, ['c0', 'c1'])), countIf(hasAll(plain, ['c0', 'c1'])),
    sum(cityHash64(id, hasAll(lc, ['c0', 'c1']))) = sum(cityHash64(id, hasAll(plain, ['c0', 'c1']))) FROM tags;
SELECT 'duplicated needle', countIf(hasAll(lc, ['c0', 'c0'])), countIf(hasAll(plain, ['c0', 'c0'])),
    sum(cityHash64(id, hasAll(lc, ['c0', 'c0']))) = sum(cityHash64(id, hasAll(plain, ['c0', 'c0']))) FROM tags;
SELECT 'empty string needle', countIf(hasAll(lc, [''])), countIf(hasAll(plain, [''])),
    sum(cityHash64(id, hasAll(lc, ['']))) = sum(cityHash64(id, hasAll(plain, ['']))) FROM tags;
SELECT '64 needles', countIf(hasAll(lc, arrayMap(j -> concat('c', toString(j % 8)), range(64)))), countIf(hasAll(plain, arrayMap(j -> concat('c', toString(j % 8)), range(64)))),
    sum(cityHash64(id, hasAll(lc, arrayMap(j -> concat('c', toString(j % 8)), range(64))))) = sum(cityHash64(id, hasAll(plain, arrayMap(j -> concat('c', toString(j % 8)), range(64))))) FROM tags;
SELECT 'needle absent from dictionary', countIf(hasAll(lc, ['c0', 'absent'])), countIf(hasAll(plain, ['c0', 'absent'])),
    sum(cityHash64(id, hasAll(lc, ['c0', 'absent']))) = sum(cityHash64(id, hasAll(plain, ['c0', 'absent']))) FROM tags;
SELECT 'no needle present', countIf(hasAll(lc, ['absent'])), countIf(hasAll(plain, ['absent'])),
    sum(cityHash64(id, hasAll(lc, ['absent']))) = sum(cityHash64(id, hasAll(plain, ['absent']))) FROM tags;

SELECT 'hasAny 1 needle', countIf(hasAny(lc, ['c0'])), countIf(hasAny(plain, ['c0'])),
    sum(cityHash64(id, hasAny(lc, ['c0']))) = sum(cityHash64(id, hasAny(plain, ['c0']))) FROM tags;
SELECT 'hasAny 8 needles', countIf(hasAny(lc, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'])), countIf(hasAny(plain, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'])),
    sum(cityHash64(id, hasAny(lc, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7']))) = sum(cityHash64(id, hasAny(plain, ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7']))) FROM tags;
SELECT 'hasAny absent and present', countIf(hasAny(lc, ['absent', 'c0'])), countIf(hasAny(plain, ['absent', 'c0'])),
    sum(cityHash64(id, hasAny(lc, ['absent', 'c0']))) = sum(cityHash64(id, hasAny(plain, ['absent', 'c0']))) FROM tags;
SELECT 'hasAny no needle present', countIf(hasAny(lc, ['absent'])), countIf(hasAny(plain, ['absent'])),
    sum(cityHash64(id, hasAny(lc, ['absent']))) = sum(cityHash64(id, hasAny(plain, ['absent']))) FROM tags;

SELECT 'LowCardinality needle', countIf(hasAll(lc, CAST(['c0', 'c1'] AS Array(LowCardinality(String))))), countIf(hasAll(plain, CAST(['c0', 'c1'] AS Array(LowCardinality(String))))),
    sum(cityHash64(id, hasAll(lc, CAST(['c0', 'c1'] AS Array(LowCardinality(String)))))) = sum(cityHash64(id, hasAll(plain, CAST(['c0', 'c1'] AS Array(LowCardinality(String)))))) FROM tags;

SELECT 'small blocks', countIf(hasAll(lc, ['c0', 'c1'])), countIf(hasAll(plain, ['c0', 'c1'])),
    sum(cityHash64(id, hasAll(lc, ['c0', 'c1']))) = sum(cityHash64(id, hasAll(plain, ['c0', 'c1']))) FROM tags SETTINGS max_block_size = 128;
SELECT 'single row blocks', countIf(hasAny(lc, ['c0', 'c1'])), countIf(hasAny(plain, ['c0', 'c1'])),
    sum(cityHash64(id, hasAny(lc, ['c0', 'c1']))) = sum(cityHash64(id, hasAny(plain, ['c0', 'c1']))) FROM tags SETTINGS max_block_size = 1;

-- Shapes the fast path declines: they must keep answering exactly as the general path does.
SELECT 'empty needle hasAll', countIf(hasAll(lc, [])), countIf(hasAll(plain, [])),
    sum(cityHash64(id, hasAll(lc, []))) = sum(cityHash64(id, hasAll(plain, []))) FROM tags;
SELECT 'empty needle hasAny', countIf(hasAny(lc, [])), countIf(hasAny(plain, [])),
    sum(cityHash64(id, hasAny(lc, []))) = sum(cityHash64(id, hasAny(plain, []))) FROM tags;
SELECT '65 needles', countIf(hasAll(lc, arrayMap(j -> concat('c', toString(j % 8)), range(65)))), countIf(hasAll(plain, arrayMap(j -> concat('c', toString(j % 8)), range(65)))),
    sum(cityHash64(id, hasAll(lc, arrayMap(j -> concat('c', toString(j % 8)), range(65))))) = sum(cityHash64(id, hasAll(plain, arrayMap(j -> concat('c', toString(j % 8)), range(65))))) FROM tags;
SELECT 'non constant needle', countIf(hasAll(lc, materialize(['c0', 'c1']))), countIf(hasAll(plain, materialize(['c0', 'c1']))),
    sum(cityHash64(id, hasAll(lc, materialize(['c0', 'c1'])))) = sum(cityHash64(id, hasAll(plain, materialize(['c0', 'c1'])))) FROM tags;
SELECT 'constant haystack', countIf(hasAll(CAST(['c0', 'c1'] AS Array(LowCardinality(String))), materialize(if(id % 2 = 0, [toLowCardinality('c0')], [toLowCardinality('absent')])))), countIf(hasAll(['c0', 'c1'], materialize(if(id % 2 = 0, ['c0'], ['absent'])))) FROM tags;
SELECT 'hasSubstr', countIf(hasSubstr(lc, ['c0', 'c1'])), countIf(hasSubstr(plain, ['c0', 'c1'])),
    sum(cityHash64(id, hasSubstr(lc, ['c0', 'c1']))) = sum(cityHash64(id, hasSubstr(plain, ['c0', 'c1']))) FROM tags;

SELECT 'all arguments constant', hasAll(CAST(['a', 'b'] AS Array(LowCardinality(String))), ['a']), hasAny(CAST(['a', 'b'] AS Array(LowCardinality(String))), ['c']),
    hasSubstr(CAST(['a', 'b'] AS Array(LowCardinality(String))), ['b', 'a']), toTypeName(hasAll(CAST(['a', 'b'] AS Array(LowCardinality(String))), ['a']));
SELECT 'declared type', toTypeName(hasAll(lc, ['c0'])), toTypeName(hasAny(lc, ['c0'])), toTypeName(hasSubstr(lc, ['c0'])) FROM tags LIMIT 1;

DROP TABLE IF EXISTS nullable_tags;
CREATE TABLE nullable_tags (id UInt64, lc Array(LowCardinality(Nullable(String))), plain Array(Nullable(String)), lc_outer Nullable(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO nullable_tags SELECT number, arr, arr, NULL FROM (SELECT number, arrayMap(j -> if((number + j) % 4 = 0, NULL, concat('n', toString((number + j) % 5))), range(1 + number % 3)) AS arr FROM numbers(60));
SELECT 'nullable dictionary hasAll', countIf(hasAll(lc, ['n1'])), countIf(hasAll(plain, ['n1'])),
    sum(cityHash64(id, hasAll(lc, ['n1']))) = sum(cityHash64(id, hasAll(plain, ['n1']))) FROM nullable_tags;
SELECT 'nullable dictionary hasAny', countIf(hasAny(lc, ['n1', 'n2'])), countIf(hasAny(plain, ['n1', 'n2'])),
    sum(cityHash64(id, hasAny(lc, ['n1', 'n2']))) = sum(cityHash64(id, hasAny(plain, ['n1', 'n2']))) FROM nullable_tags;
SELECT 'null needle', countIf(hasAll(lc, [NULL])), countIf(hasAll(plain, [NULL])),
    sum(cityHash64(id, hasAll(lc, [NULL]))) = sum(cityHash64(id, hasAll(plain, [NULL]))) FROM nullable_tags;
SELECT 'nullable declared type', toTypeName(hasAll(lc, ['n1'])), toTypeName(hasAny(lc, ['n1'])), toTypeName(hasSubstr(lc, ['n1'])) FROM nullable_tags LIMIT 1;

DROP TABLE IF EXISTS nested_tags;
CREATE TABLE nested_tags (id UInt64, lc Array(Array(LowCardinality(String))), plain Array(Array(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO nested_tags SELECT number, arr, arr FROM (SELECT number, [arrayMap(j -> concat('g', toString((number + j) % 4)), range(2))] AS arr FROM numbers(40));
SELECT 'nested array', countIf(hasAll(lc, [['g0', 'g1']])), countIf(hasAll(plain, [['g0', 'g1']])),
    sum(cityHash64(id, hasAll(lc, [['g0', 'g1']]))) = sum(cityHash64(id, hasAll(plain, [['g0', 'g1']]))) FROM nested_tags;

DROP TABLE IF EXISTS outer_nullable_tags;
CREATE TABLE outer_nullable_tags (id UInt64, lc Nullable(String), tags Array(LowCardinality(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO outer_nullable_tags SELECT number, NULL, arrayMap(j -> concat('c', toString((number + j) % 5)), range(2)) FROM numbers(40);
SELECT 'variant array argument', countIf(hasAll(if(id % 2 = 0, tags, NULL), ['c0'])), toTypeName(if(id % 2 = 0, tags, NULL)), toTypeName(hasAll(if(id % 2 = 0, tags, NULL), ['c0'])) FROM outer_nullable_tags;

-- One accepting and one declining row per element type, against the plain twin.
DROP TABLE IF EXISTS typed;
CREATE TABLE typed
(
    id UInt64,
    fs_lc Array(LowCardinality(FixedString(3))), fs_plain Array(FixedString(3)),
    u16_lc Array(LowCardinality(UInt16)), u16_plain Array(UInt16),
    i64_lc Array(LowCardinality(Int64)), i64_plain Array(Int64),
    date_lc Array(LowCardinality(Date)), date_plain Array(Date),
    uuid_lc Array(LowCardinality(UUID)), uuid_plain Array(UUID),
    ipv4_lc Array(LowCardinality(IPv4)), ipv4_plain Array(IPv4),
    f64_lc Array(LowCardinality(Float64)), f64_plain Array(Float64)
) ENGINE = MergeTree ORDER BY id;
INSERT INTO typed
SELECT number, fs, fs, u16, u16, i64, i64, d, d, uu, uu, ip, ip, f64, f64
FROM
(
    SELECT
        number,
        arrayMap(j -> toFixedString(concat('k', toString((number + j) % 6)), 3), range(1 + number % 3)) AS fs,
        arrayMap(j -> toUInt16((number + j) % 11), range(1 + number % 3)) AS u16,
        arrayMap(j -> toInt64(-1 - ((number + j) % 11)), range(1 + number % 3)) AS i64,
        arrayMap(j -> toDate('2026-01-01') + ((number + j) % 9), range(1 + number % 3)) AS d,
        arrayMap(j -> toUUID(concat('00000000-0000-0000-0000-00000000000', toString((number + j) % 9))), range(1 + number % 3)) AS uu,
        arrayMap(j -> toIPv4(concat('10.0.0.', toString((number + j) % 9))), range(1 + number % 3)) AS ip,
        arrayConcat(arrayMap(j -> toFloat64((number + j) % 9), range(1 + number % 3)), if(number % 4 = 0, [nan], []), if(number % 6 = 0, [inf, -inf], [])) AS f64
    FROM numbers(120)
);

SELECT 'FixedString matching needle', countIf(hasAll(fs_lc, [toFixedString('k1', 3)])), countIf(hasAll(fs_plain, [toFixedString('k1', 3)])),
    sum(cityHash64(id, hasAll(fs_lc, [toFixedString('k1', 3)]))) = sum(cityHash64(id, hasAll(fs_plain, [toFixedString('k1', 3)]))) FROM typed;
SELECT 'FixedString hasAny', countIf(hasAny(fs_lc, [toFixedString('k1', 3), toFixedString('k2', 3)])), countIf(hasAny(fs_plain, [toFixedString('k1', 3), toFixedString('k2', 3)])),
    sum(cityHash64(id, hasAny(fs_lc, [toFixedString('k1', 3), toFixedString('k2', 3)]))) = sum(cityHash64(id, hasAny(fs_plain, [toFixedString('k1', 3), toFixedString('k2', 3)]))) FROM typed;
SELECT 'FixedString hasSubstr', countIf(hasSubstr(fs_lc, [toFixedString('k1', 3)])), countIf(hasSubstr(fs_plain, [toFixedString('k1', 3)])),
    sum(cityHash64(id, hasSubstr(fs_lc, [toFixedString('k1', 3)]))) = sum(cityHash64(id, hasSubstr(fs_plain, [toFixedString('k1', 3)]))) FROM typed;
SELECT 'FixedString String needle', countIf(hasAll(fs_lc, ['k1'])), countIf(hasAll(fs_plain, ['k1'])),
    sum(cityHash64(id, hasAll(fs_lc, ['k1']))) = sum(cityHash64(id, hasAll(fs_plain, ['k1']))) FROM typed;
SELECT 'FixedString NUL tail needle', countIf(hasAll(fs_lc, ['k1\0'])), countIf(hasAll(fs_plain, ['k1\0'])),
    sum(cityHash64(id, hasAll(fs_lc, ['k1\0']))) = sum(cityHash64(id, hasAll(fs_plain, ['k1\0']))) FROM typed;

SELECT 'UInt16 matching needle', countIf(hasAll(u16_lc, [toUInt16(3)])), countIf(hasAll(u16_plain, [toUInt16(3)])),
    sum(cityHash64(id, hasAll(u16_lc, [toUInt16(3)]))) = sum(cityHash64(id, hasAll(u16_plain, [toUInt16(3)]))) FROM typed;
SELECT 'UInt16 hasAny', countIf(hasAny(u16_lc, [toUInt16(3), toUInt16(4)])), countIf(hasAny(u16_plain, [toUInt16(3), toUInt16(4)])),
    sum(cityHash64(id, hasAny(u16_lc, [toUInt16(3), toUInt16(4)]))) = sum(cityHash64(id, hasAny(u16_plain, [toUInt16(3), toUInt16(4)]))) FROM typed;
SELECT 'UInt16 hasSubstr', countIf(hasSubstr(u16_lc, [toUInt16(3)])), countIf(hasSubstr(u16_plain, [toUInt16(3)])),
    sum(cityHash64(id, hasSubstr(u16_lc, [toUInt16(3)]))) = sum(cityHash64(id, hasSubstr(u16_plain, [toUInt16(3)]))) FROM typed;
SELECT 'UInt16 widening needle', countIf(hasAll(u16_lc, [3])), countIf(hasAll(u16_plain, [3])),
    sum(cityHash64(id, hasAll(u16_lc, [3]))) = sum(cityHash64(id, hasAll(u16_plain, [3]))) FROM typed;
SELECT 'UInt16 out of range needle', countIf(hasAny(u16_lc, [toInt32(-1), toInt32(3)])), countIf(hasAny(u16_plain, [toInt32(-1), toInt32(3)])),
    sum(cityHash64(id, hasAny(u16_lc, [toInt32(-1), toInt32(3)]))) = sum(cityHash64(id, hasAny(u16_plain, [toInt32(-1), toInt32(3)]))) FROM typed;

SELECT 'Int64 matching needle', countIf(hasAll(i64_lc, [toInt64(-4)])), countIf(hasAll(i64_plain, [toInt64(-4)])),
    sum(cityHash64(id, hasAll(i64_lc, [toInt64(-4)]))) = sum(cityHash64(id, hasAll(i64_plain, [toInt64(-4)]))) FROM typed;
SELECT 'Int64 hasAny', countIf(hasAny(i64_lc, [toInt64(-4), toInt64(-5)])), countIf(hasAny(i64_plain, [toInt64(-4), toInt64(-5)])),
    sum(cityHash64(id, hasAny(i64_lc, [toInt64(-4), toInt64(-5)]))) = sum(cityHash64(id, hasAny(i64_plain, [toInt64(-4), toInt64(-5)]))) FROM typed;
SELECT 'Int64 hasSubstr', countIf(hasSubstr(i64_lc, [toInt64(-4)])), countIf(hasSubstr(i64_plain, [toInt64(-4)])),
    sum(cityHash64(id, hasSubstr(i64_lc, [toInt64(-4)]))) = sum(cityHash64(id, hasSubstr(i64_plain, [toInt64(-4)]))) FROM typed;

SELECT 'Date matching needle', countIf(hasAll(date_lc, [toDate('2026-01-04')])), countIf(hasAll(date_plain, [toDate('2026-01-04')])),
    sum(cityHash64(id, hasAll(date_lc, [toDate('2026-01-04')]))) = sum(cityHash64(id, hasAll(date_plain, [toDate('2026-01-04')]))) FROM typed;
SELECT 'Date hasAny', countIf(hasAny(date_lc, [toDate('2026-01-04'), toDate('2026-01-05')])), countIf(hasAny(date_plain, [toDate('2026-01-04'), toDate('2026-01-05')])),
    sum(cityHash64(id, hasAny(date_lc, [toDate('2026-01-04'), toDate('2026-01-05')]))) = sum(cityHash64(id, hasAny(date_plain, [toDate('2026-01-04'), toDate('2026-01-05')]))) FROM typed;
SELECT 'Date hasSubstr', countIf(hasSubstr(date_lc, [toDate('2026-01-04')])), countIf(hasSubstr(date_plain, [toDate('2026-01-04')])),
    sum(cityHash64(id, hasSubstr(date_lc, [toDate('2026-01-04')]))) = sum(cityHash64(id, hasSubstr(date_plain, [toDate('2026-01-04')]))) FROM typed;
SELECT 'Date DateTime needle', countIf(hasAny(date_lc, [toDateTime('2026-01-04 00:00:00')])), countIf(hasAny(date_plain, [toDateTime('2026-01-04 00:00:00')])),
    sum(cityHash64(id, hasAny(date_lc, [toDateTime('2026-01-04 00:00:00')]))) = sum(cityHash64(id, hasAny(date_plain, [toDateTime('2026-01-04 00:00:00')]))) FROM typed;
SELECT 'Date DateTime needle with time', countIf(hasAny(date_lc, [toDateTime('2026-01-04 12:00:00')])), countIf(hasAny(date_plain, [toDateTime('2026-01-04 12:00:00')])),
    sum(cityHash64(id, hasAny(date_lc, [toDateTime('2026-01-04 12:00:00')]))) = sum(cityHash64(id, hasAny(date_plain, [toDateTime('2026-01-04 12:00:00')]))) FROM typed;

SELECT 'UUID matching needle', countIf(hasAll(uuid_lc, [toUUID('00000000-0000-0000-0000-000000000003')])), countIf(hasAll(uuid_plain, [toUUID('00000000-0000-0000-0000-000000000003')])),
    sum(cityHash64(id, hasAll(uuid_lc, [toUUID('00000000-0000-0000-0000-000000000003')]))) = sum(cityHash64(id, hasAll(uuid_plain, [toUUID('00000000-0000-0000-0000-000000000003')]))) FROM typed;
SELECT 'UUID hasAny', countIf(hasAny(uuid_lc, [toUUID('00000000-0000-0000-0000-000000000003'), toUUID('00000000-0000-0000-0000-000000000004')])), countIf(hasAny(uuid_plain, [toUUID('00000000-0000-0000-0000-000000000003'), toUUID('00000000-0000-0000-0000-000000000004')])),
    sum(cityHash64(id, hasAny(uuid_lc, [toUUID('00000000-0000-0000-0000-000000000003'), toUUID('00000000-0000-0000-0000-000000000004')]))) = sum(cityHash64(id, hasAny(uuid_plain, [toUUID('00000000-0000-0000-0000-000000000003'), toUUID('00000000-0000-0000-0000-000000000004')]))) FROM typed;
SELECT 'UUID hasSubstr', countIf(hasSubstr(uuid_lc, [toUUID('00000000-0000-0000-0000-000000000003')])), countIf(hasSubstr(uuid_plain, [toUUID('00000000-0000-0000-0000-000000000003')])),
    sum(cityHash64(id, hasSubstr(uuid_lc, [toUUID('00000000-0000-0000-0000-000000000003')]))) = sum(cityHash64(id, hasSubstr(uuid_plain, [toUUID('00000000-0000-0000-0000-000000000003')]))) FROM typed;

SELECT 'IPv4 matching needle', countIf(hasAll(ipv4_lc, [toIPv4('10.0.0.3')])), countIf(hasAll(ipv4_plain, [toIPv4('10.0.0.3')])),
    sum(cityHash64(id, hasAll(ipv4_lc, [toIPv4('10.0.0.3')]))) = sum(cityHash64(id, hasAll(ipv4_plain, [toIPv4('10.0.0.3')]))) FROM typed;
SELECT 'IPv4 hasAny', countIf(hasAny(ipv4_lc, [toIPv4('10.0.0.3'), toIPv4('10.0.0.4')])), countIf(hasAny(ipv4_plain, [toIPv4('10.0.0.3'), toIPv4('10.0.0.4')])),
    sum(cityHash64(id, hasAny(ipv4_lc, [toIPv4('10.0.0.3'), toIPv4('10.0.0.4')]))) = sum(cityHash64(id, hasAny(ipv4_plain, [toIPv4('10.0.0.3'), toIPv4('10.0.0.4')]))) FROM typed;
SELECT 'IPv4 hasSubstr', countIf(hasSubstr(ipv4_lc, [toIPv4('10.0.0.3')])), countIf(hasSubstr(ipv4_plain, [toIPv4('10.0.0.3')])),
    sum(cityHash64(id, hasSubstr(ipv4_lc, [toIPv4('10.0.0.3')]))) = sum(cityHash64(id, hasSubstr(ipv4_plain, [toIPv4('10.0.0.3')]))) FROM typed;

-- A float dictionary is declined: NaN is one dictionary entry but equals no value, so answering from
-- dictionary identity would report matches the general path does not make. Removing the gate reddens these.
SELECT 'Float64 nan hasAll', countIf(hasAll(f64_lc, [nan])), countIf(hasAll(f64_plain, [nan])),
    sum(cityHash64(id, hasAll(f64_lc, [nan]))) = sum(cityHash64(id, hasAll(f64_plain, [nan]))) FROM typed;
SELECT 'Float64 nan hasAny', countIf(hasAny(f64_lc, [nan])), countIf(hasAny(f64_plain, [nan])),
    sum(cityHash64(id, hasAny(f64_lc, [nan]))) = sum(cityHash64(id, hasAny(f64_plain, [nan]))) FROM typed;
SELECT 'Float64 nan and value hasAll', countIf(hasAll(f64_lc, [nan, toFloat64(3)])), countIf(hasAll(f64_plain, [nan, toFloat64(3)])),
    sum(cityHash64(id, hasAll(f64_lc, [nan, toFloat64(3)]))) = sum(cityHash64(id, hasAll(f64_plain, [nan, toFloat64(3)]))) FROM typed;
SELECT 'Float64 nan hasSubstr', countIf(hasSubstr(f64_lc, [nan])), countIf(hasSubstr(f64_plain, [nan])),
    sum(cityHash64(id, hasSubstr(f64_lc, [nan]))) = sum(cityHash64(id, hasSubstr(f64_plain, [nan]))) FROM typed;
SELECT 'Float64 zero hasAll', countIf(hasAll(f64_lc, [toFloat64(0)])), countIf(hasAll(f64_plain, [toFloat64(0)])),
    sum(cityHash64(id, hasAll(f64_lc, [toFloat64(0)]))) = sum(cityHash64(id, hasAll(f64_plain, [toFloat64(0)]))) FROM typed;
SELECT 'Float64 inf hasAny', countIf(hasAny(f64_lc, [inf, -inf])), countIf(hasAny(f64_plain, [inf, -inf])),
    sum(cityHash64(id, hasAny(f64_lc, [inf, -inf]))) = sum(cityHash64(id, hasAny(f64_plain, [inf, -inf]))) FROM typed;
SELECT 'Float64 value hasAll', countIf(hasAll(f64_lc, [toFloat64(3)])), countIf(hasAll(f64_plain, [toFloat64(3)])),
    sum(cityHash64(id, hasAll(f64_lc, [toFloat64(3)]))) = sum(cityHash64(id, hasAll(f64_plain, [toFloat64(3)]))) FROM typed;
SELECT 'Float64 has nan reference', countIf(has(f64_lc, nan)), countIf(has(f64_plain, nan)) FROM typed;

DROP TABLE IF EXISTS small_dictionary;
CREATE TABLE small_dictionary (id UInt64, lc Array(LowCardinality(String)), plain Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO small_dictionary SELECT number, arr, arr FROM (SELECT number, arrayMap(j -> concat('s', toString((number + j) % 40)), range(1 + number % 4)) AS arr FROM numbers(200));
SELECT 'small dictionary', countIf(hasAll(lc, ['s3', 's4'])), countIf(hasAll(plain, ['s3', 's4'])),
    sum(cityHash64(id, hasAll(lc, ['s3', 's4']))) = sum(cityHash64(id, hasAll(plain, ['s3', 's4']))) FROM small_dictionary;
SELECT 'small dictionary hasAny', countIf(hasAny(lc, ['s3', 's4'])), countIf(hasAny(plain, ['s3', 's4'])),
    sum(cityHash64(id, hasAny(lc, ['s3', 's4']))) = sum(cityHash64(id, hasAny(plain, ['s3', 's4']))) FROM small_dictionary;

-- MergeTreeIndexBloomFilter coerces a hasAll/hasAny constant to the least supertype and hashes that,
-- so pruning and the function must agree: the two counts per row differ if they do not.
-- force_data_skipping_indices names the one index each arm must have found useful: without it
-- use_skip_indexes = 1 merely permits pruning, so an arm passes even with the index never consulted.
DROP TABLE IF EXISTS indexed_string;
CREATE TABLE indexed_string (id UInt64, lc Array(LowCardinality(String)), plain Array(String), INDEX bf lc TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO indexed_string SELECT number, arr, arr FROM (SELECT number, arrayMap(j -> concat('b', toString((number + j) % 12)), range(1 + number % 3)) AS arr FROM numbers(200));
SELECT 'bloom_filter hasAll', (SELECT count() FROM indexed_string WHERE hasAll(lc, ['b3', 'b4']) SETTINGS use_skip_indexes = 0), (SELECT count() FROM indexed_string WHERE hasAll(lc, ['b3', 'b4']) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'bf'), (SELECT count() FROM indexed_string WHERE hasAll(plain, ['b3', 'b4']));
SELECT 'bloom_filter hasAny', (SELECT count() FROM indexed_string WHERE hasAny(lc, ['b3', 'b4']) SETTINGS use_skip_indexes = 0), (SELECT count() FROM indexed_string WHERE hasAny(lc, ['b3', 'b4']) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'bf'), (SELECT count() FROM indexed_string WHERE hasAny(plain, ['b3', 'b4']));
SELECT 'bloom_filter absent needle', (SELECT count() FROM indexed_string WHERE hasAny(lc, ['absent']) SETTINGS use_skip_indexes = 0), (SELECT count() FROM indexed_string WHERE hasAny(lc, ['absent']) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'bf');

DROP TABLE IF EXISTS indexed_text;
CREATE TABLE indexed_text (id UInt64, lc Array(LowCardinality(String)), plain Array(String), INDEX txt lc TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO indexed_text SELECT number, arr, arr FROM (SELECT number, arrayMap(j -> concat('b', toString((number + j) % 12)), range(1 + number % 3)) AS arr FROM numbers(200));
SELECT 'text index hasAll', (SELECT count() FROM indexed_text WHERE hasAll(lc, ['b3', 'b4']) SETTINGS use_skip_indexes = 0), (SELECT count() FROM indexed_text WHERE hasAll(lc, ['b3', 'b4']) SETTINGS use_skip_indexes = 1, use_skip_indexes_if_final = 1, force_data_skipping_indices = 'txt');

DROP TABLE IF EXISTS indexed_fixed_string;
CREATE TABLE indexed_fixed_string (id UInt64, lc Array(LowCardinality(FixedString(3))), plain Array(FixedString(3)), INDEX bf lc TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO indexed_fixed_string SELECT number, arr, arr FROM (SELECT number, arrayMap(j -> toFixedString(concat('p', toString((number + j) % 12)), 3), range(1 + number % 3)) AS arr FROM numbers(200));
SELECT 'bloom_filter FixedString needle', (SELECT count() FROM indexed_fixed_string WHERE hasAll(lc, [toFixedString('p3', 3)]) SETTINGS use_skip_indexes = 0), (SELECT count() FROM indexed_fixed_string WHERE hasAll(lc, [toFixedString('p3', 3)]) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'bf'), (SELECT count() FROM indexed_fixed_string WHERE hasAll(plain, [toFixedString('p3', 3)]));
SELECT 'bloom_filter String needle', (SELECT count() FROM indexed_fixed_string WHERE hasAll(lc, ['p3']) SETTINGS use_skip_indexes = 0), (SELECT count() FROM indexed_fixed_string WHERE hasAll(lc, ['p3']) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'bf'), (SELECT count() FROM indexed_fixed_string WHERE hasAll(plain, ['p3']));
SELECT 'bloom_filter NUL tail needle', (SELECT count() FROM indexed_fixed_string WHERE hasAny(lc, ['p3\0']) SETTINGS use_skip_indexes = 0), (SELECT count() FROM indexed_fixed_string WHERE hasAny(lc, ['p3\0']) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'bf'), (SELECT count() FROM indexed_fixed_string WHERE hasAny(plain, ['p3\0']));

DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS nullable_tags;
DROP TABLE IF EXISTS nested_tags;
DROP TABLE IF EXISTS outer_nullable_tags;
DROP TABLE IF EXISTS typed;
DROP TABLE IF EXISTS small_dictionary;
DROP TABLE IF EXISTS indexed_string;
DROP TABLE IF EXISTS indexed_text;
DROP TABLE IF EXISTS indexed_fixed_string;

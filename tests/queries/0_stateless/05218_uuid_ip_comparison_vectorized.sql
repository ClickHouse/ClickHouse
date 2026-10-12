-- IPv6 values are chosen so that byte-wise (network) order differs from little-endian UInt128 order.

DROP TABLE IF EXISTS t_cmp_uuid_ip;
CREATE TABLE t_cmp_uuid_ip
(
    id UInt8,
    u1 UUID, u2 UUID,
    i4a IPv4, i4b IPv4,
    i6a IPv6, i6b IPv6,
    nu Nullable(UUID),
    n6 Nullable(IPv6),
    lu LowCardinality(UUID)
) ENGINE = Memory;

INSERT INTO t_cmp_uuid_ip VALUES
    (0, '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', '0.0.0.0', '0.0.0.0', '::', '::', NULL, NULL, '00000000-0000-0000-0000-000000000000'),
    (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000000', '0.0.0.1', '0.0.0.0', '::1', '::', '00000000-0000-0000-0000-000000000001', '::1', '00000000-0000-0000-0000-000000000001'),
    (2, '00000000-0000-0001-0000-000000000000', '00000000-0000-0000-ffff-ffffffffffff', '255.255.255.255', '0.0.0.1', '1::', '::1', '00000000-0000-0001-0000-000000000000', '1::', '00000000-0000-0001-0000-000000000000'),
    (3, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 'ffffffff-ffff-ffff-ffff-ffffffffffff', '127.0.0.1', '127.0.0.1', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', NULL, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
    (4, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba1', '192.168.1.1', '10.0.0.1', '2001:db8::1', '2001:db8::2', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '2001:db8::1', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'),
    (5, '61f0c404-5cb3-11e7-907b-a6006ad3dba1', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '10.0.0.1', '192.168.1.1', '2001:db8::2', '2001:db8::1', '61f0c404-5cb3-11e7-907b-a6006ad3dba1', '2001:db8::2', '61f0c404-5cb3-11e7-907b-a6006ad3dba1'),
    (6, '00000000-0000-0000-8000-000000000000', '80000000-0000-0000-0000-000000000000', '128.0.0.0', '0.0.0.128', '8000::', '::8000', NULL, '8000::', '00000000-0000-0000-8000-000000000000'),
    (7, '80000000-0000-0000-0000-000000000000', '00000000-0000-0000-8000-000000000000', '0.0.0.128', '128.0.0.0', '::8000', '8000::', '80000000-0000-0000-0000-000000000000', '::8000', '80000000-0000-0000-0000-000000000000'),
    (8, '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000001', '1.2.3.4', '1.2.3.5', '::ffff:1.2.3.4', '::ffff:1.2.3.5', '00000000-0000-0000-0000-000000000000', '::ffff:1.2.3.4', '00000000-0000-0000-0000-000000000000');

SELECT 'vector-vector';
SELECT id, u1 = u2, u1 != u2, u1 < u2, u1 > u2, u1 <= u2, u1 >= u2 FROM t_cmp_uuid_ip ORDER BY id;
SELECT id, i4a = i4b, i4a != i4b, i4a < i4b, i4a > i4b, i4a <= i4b, i4a >= i4b FROM t_cmp_uuid_ip ORDER BY id;
SELECT id, i6a = i6b, i6a != i6b, i6a < i6b, i6a > i6b, i6a <= i6b, i6a >= i6b FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'vector-constant';
SELECT id, u1 = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), u1 != toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), u1 < toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), u1 > toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), u1 <= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), u1 >= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') FROM t_cmp_uuid_ip ORDER BY id;
SELECT id, i4a = toIPv4('127.0.0.1'), i4a != toIPv4('127.0.0.1'), i4a < toIPv4('127.0.0.1'), i4a > toIPv4('127.0.0.1'), i4a <= toIPv4('127.0.0.1'), i4a >= toIPv4('127.0.0.1') FROM t_cmp_uuid_ip ORDER BY id;
SELECT id, i6a = toIPv6('2001:db8::1'), i6a != toIPv6('2001:db8::1'), i6a < toIPv6('2001:db8::1'), i6a > toIPv6('2001:db8::1'), i6a <= toIPv6('2001:db8::1'), i6a >= toIPv6('2001:db8::1') FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'constant-vector';
SELECT id, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') = u1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') != u1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') < u1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') > u1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') <= u1, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') >= u1 FROM t_cmp_uuid_ip ORDER BY id;
SELECT id, toIPv4('127.0.0.1') = i4a, toIPv4('127.0.0.1') != i4a, toIPv4('127.0.0.1') < i4a, toIPv4('127.0.0.1') > i4a, toIPv4('127.0.0.1') <= i4a, toIPv4('127.0.0.1') >= i4a FROM t_cmp_uuid_ip ORDER BY id;
SELECT id, toIPv6('2001:db8::1') = i6a, toIPv6('2001:db8::1') != i6a, toIPv6('2001:db8::1') < i6a, toIPv6('2001:db8::1') > i6a, toIPv6('2001:db8::1') <= i6a, toIPv6('2001:db8::1') >= i6a FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'constant-constant';
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') < toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1'), toIPv4('1.2.3.4') = toIPv4('1.2.3.4'), toIPv6('::1') < toIPv6('1::'), toIPv6('::8000') > toIPv6('8000::');

SELECT 'string literals';
SELECT id, u1 = '61f0c404-5cb3-11e7-907b-a6006ad3dba0', i4a > '127.0.0.1', i6a < '2001:db8::1' FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'nullable';
SELECT id, nu = u1, nu < u1, u2 >= nu, nu = nu, n6 < i6a, n6 != toIPv6('::1'), toIPv6('8000::') <= n6 FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'low cardinality';
SELECT id, lu = u1, lu < u2, lu > toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'mixed ipv4 / ipv6 / fixedstring keep the cast path';
SELECT id, i4a = i6a, i6a = i4a, i4a < i6b, i6a = toFixedString(unhex('20010db8000000000000000000000001'), 16), toIPv4('1.2.3.4') = i6a FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'tuples';
SELECT id, (u1, i4a) < (u2, i4b), (i6a, u1) = (i6b, u2), (u1, i6a) >= (u1, i6b) FROM t_cmp_uuid_ip ORDER BY id;

SELECT 'consistency with sort order';
SELECT count() FROM (SELECT arraySort(groupArray(u1)) AS a FROM t_cmp_uuid_ip) ARRAY JOIN arrayZip(arrayPopBack(a), arrayPopFront(a)) AS p WHERE NOT (p.1 <= p.2 AND (p.1 < p.2 OR p.1 = p.2) AND p.2 >= p.1);
SELECT count() FROM (SELECT arraySort(groupArray(i6a)) AS a FROM t_cmp_uuid_ip) ARRAY JOIN arrayZip(arrayPopBack(a), arrayPopFront(a)) AS p WHERE NOT (p.1 <= p.2 AND (p.1 < p.2 OR p.1 = p.2) AND p.2 >= p.1);
SELECT count() FROM (SELECT arraySort(groupArray(i4a)) AS a FROM t_cmp_uuid_ip) ARRAY JOIN arrayZip(arrayPopBack(a), arrayPopFront(a)) AS p WHERE NOT (p.1 <= p.2 AND (p.1 < p.2 OR p.1 = p.2) AND p.2 >= p.1);

SELECT 'bulk';
WITH
    reinterpretAsUUID(concat(reinterpretAsFixedString(cityHash64(number, 1)), reinterpretAsFixedString(cityHash64(number, 2)))) AS a,
    reinterpretAsUUID(concat(reinterpretAsFixedString(cityHash64(number, if(number % 3 = 0, 1, 3))), reinterpretAsFixedString(cityHash64(number, if(number % 2 = 0, 2, 4))))) AS b,
    CAST(reinterpretAsFixedString(a) AS IPv6) AS x,
    CAST(reinterpretAsFixedString(b) AS IPv6) AS y,
    toIPv4(cityHash64(number, 5) % 65536) AS p,
    toIPv4(cityHash64(number, if(number % 2 = 0, 5, 6)) % 65536) AS q
SELECT
    countIf(a = b), countIf(a != b), countIf(a < b), countIf(a > b), countIf(a <= b), countIf(a >= b),
    countIf(x = y), countIf(x != y), countIf(x < y), countIf(x > y), countIf(x <= y), countIf(x >= y),
    countIf(p = q), countIf(p != q), countIf(p < q), countIf(p > q), countIf(p <= q), countIf(p >= q),
    countIf(a < toUUID('a08e8cc1-9fed-0d02-c6d6-b0d7dd1220d7')), countIf(x < toIPv6('20d:ed9f:c18c:8ea0:d720:12dd:d7b0:d6c6')), countIf(p >= toIPv4('0.0.128.0')),
    countIf(toUUID('a08e8cc1-9fed-0d02-c6d6-b0d7dd1220d7') <= a), countIf(toIPv6('20d:ed9f:c18c:8ea0:d720:12dd:d7b0:d6c6') > x), countIf(toIPv4('0.0.128.0') != p),
    countIf(a = toUUID('a08e8cc1-9fed-0d02-c6d6-b0d7dd1220d7')), countIf(x = toIPv6('20d:ed9f:c18c:8ea0:d720:12dd:d7b0:d6c6')),
    countIf((a < b) != NOT (a >= b)), countIf((x < y) != NOT (x >= y)), countIf((p < q) != NOT (p >= q)),
    countIf((a = b) != NOT (a != b)), countIf((x = y) != NOT (x != y)), countIf((p = q) != NOT (p != q)),
    countIf((a < b) != (b > a)), countIf((x < y) != (y > x)), countIf((p < q) != (q > p))
FROM numbers(100000);

DROP TABLE t_cmp_uuid_ip;

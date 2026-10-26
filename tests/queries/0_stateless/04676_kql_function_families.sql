-- The binary, IP and point-geospatial function families.
--
-- Every expected value below is the one printed in Microsoft's own reference page for that
-- function, so this file is a conformance check rather than a record of what we happen to do.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- binary --';
print binary_and(6, 3);                 // 2
print binary_or(5, 3);                  // 7
print binary_xor(1, 1);                 // 0
print binary_xor(1, 2);                 // 3
print binary_not(100);                  // -101
print binary_shift_left(1, 2);          // 4
print binary_shift_right(1, 2);         // 0
print bitset_count_ones(42);            // 3
-- Kusto shifts by `n % 64`, and answers null for a negative shift.
print binary_shift_left(1, 64);         // 1
print binary_shift_left(1, -1);         // null

print '-- tohex --';
print tohex(256);                       // 100
print tohex(-256);                      // ffffffffffffff00
print tohex(256, 8);                    // 00000100
print tohex(256, 2);                    // 100, minLength never truncates

print '-- strcmp --';
datatable (a:string, b:string) ['ABC','ABC', 'abc','ABC', 'ABC','abc', 'abcde','abc']
| extend result = strcmp(a, b);

print '-- datetime_add --';
print datetime_add('year', 1, make_datetime(2017,1,1));
print datetime_add('quarter', 1, make_datetime(2017,1,1));
print datetime_add('month', 1, make_datetime(2017,1,1));
print datetime_add('week', 1, make_datetime(2017,1,1));
print datetime_add('day', 1, make_datetime(2017,1,1));
print datetime_add('year', -5, make_datetime(2017,1,1));

print '-- datetime_part, including the cumulative sub-second parts --';
print datetime_part('year', datetime(2017-10-30 01:02:03.7654321));          // 2017
print datetime_part('quarter', datetime(2017-10-30 01:02:03.7654321));       // 4
print datetime_part('week_of_year', datetime(2017-10-30 01:02:03.7654321));  // 44
print datetime_part('dayOfYear', datetime(2017-10-30 01:02:03.7654321));     // 303
print datetime_part('millisecond', datetime(2017-10-30 01:02:03.7654321));   // 765
print datetime_part('microsecond', datetime(2017-10-30 01:02:03.7654321));   // 765432
print datetime_part('nanosecond', datetime(2017-10-30 01:02:03.7654321));    // 765432100

print '-- make_timespan --';
SET interval_output_format = 'kusto';
print make_timespan(1, 12);             // 01:12:00
print make_timespan(1, 12, 30, 55.123); // 1.12:30:55.1230000
print dayofweek(datetime(2015-12-14));  // 1.00:00:00 -- a timespan, not an integer
SET interval_output_format = 'numeric';

print '-- IPv4: a CIDR suffix in the string masks the low bits --';
datatable (ip:string) ['192.168.1.1', '192.168.1.1/24', '255.255.255.255/31']
| extend value = parse_ipv4(ip);
print parse_ipv4_mask('127.0.0.1', 24);        // 2130706432

print '-- IPv4 formatting --';
datatable (address:string, mask:long) ['192.168.1.1', 24, '192.168.1.1', 32, '192.168.1.1/24', 32, '192.168.1.1/24', -1]
| extend result = format_ipv4(address, mask), result_mask = format_ipv4_mask(address, mask);

print '-- IPv4 comparison uses the narrowest of the prefixes involved --';
datatable (a:string, b:string)
['192.168.1.0','192.168.1.0', '192.168.1.1/24','192.168.1.255', '192.168.1.1','192.168.1.255/24', '192.168.1.1/30','192.168.1.255/24']
| extend result = ipv4_compare(a, b);
print ipv4_is_match('192.168.1.1/24', '192.168.1.255');   // true

print '-- IPv4 ranges --';
datatable (ip:string, range:string) ['192.168.1.1','192.168.1.1', '192.168.1.1','192.168.1.255/24']
| extend result = ipv4_is_in_range(ip, range);
print ipv4_is_in_any_range('192.168.1.6', '192.168.1.1/24', '10.0.0.1/8', '127.1.0.1/16');   // true

print '-- ipv4_is_private is RFC 1918 only, so loopback is not private --';
datatable (ip:string) ['10.1.2.3', '192.168.1.1/24', '127.0.0.1']
| extend result = ipv4_is_private(ip);

print '-- ipv4_netmask_suffix defaults to 32 --';
datatable (ip:string) ['10.1.2.3', '192.168.1.1/24', '127.0.0.1/16']
| extend suffix = ipv4_netmask_suffix(ip);

print '-- IPv6, which also accepts IPv4 strings on either side --';
print ipv6_is_match('192.168.1.1', '::ffff:c0a8:0101');    // true
print ipv6_compare('192.168.1.1', '::ffff:c0a8:0101');     // 0
print ipv6_is_in_range('a5e::abcd', 'a5e::0/112');         // true
print ipv6_is_in_range('a5e::abcd', '0:0:0:0:0:ffff:c0a8:ac/60');  // false

print '-- geospatial: longitude before latitude, distances in metres --';
-- ClickHouse's fast approximation, so this is close to but not identical with the
-- 1546754.35197381 Kusto prints. `use_spheroid` picks the ellipsoid formula instead.
print geo_distance_2points(-122.407628, 47.578557, -118.275287, 34.019056);
print geo_distance_2points(-122.407628, 47.578557, -118.275287, 34.019056, true);
print geo_point_in_circle(-122.143564, 47.535677, -122.100896, 47.527351, 3500);  // true
print geo_point_in_circle(-122.137575, 47.630683, -122.100896, 47.527351, 3500);  // false

-- The H3 cells are in 04693_kql_geo_h3, which the fast test skips: it has no H3 library.
print '-- a geohash is a token string --';
print geo_point_to_geohash(-80.195829, 25.802215, 8);      // dhwfz15h

print '-- more mathematics --';
print gamma(5);          // 24
// The last digit of the raw quotient differs across libm implementations.
print round(cot(1), 10);
print pi();
print isascii('abc');
print isutf8('abc');

SET dialect = 'clickhouse';

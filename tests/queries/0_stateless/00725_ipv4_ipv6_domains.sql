DROP TABLE IF EXISTS ipv4_test;

-- Only valid values for IPv4
CREATE TABLE ipv4_test (ipv4_ IPv4) ENGINE = Memory;

-- ipv4_ column shoud have type 'IPv4'
SHOW CREATE TABLE ipv4_test;

INSERT INTO ipv4_test (ipv4_) VALUES ('0.0.0.0'), ('255.255.255.255'), ('192.168.0.91'), ('127.0.0.1'), ('8.8.8.8');

SELECT ipv4_, hex(ipv4_) FROM ipv4_test ORDER BY ipv4_;

SELECT '< 127.0.0.1', ipv4_ FROM ipv4_test
    WHERE ipv4_ < toIPv4('127.0.0.1')
    ORDER BY ipv4_;

SELECT '> 127.0.0.1', ipv4_ FROM ipv4_test
    WHERE ipv4_ > toIPv4('127.0.0.1')
    ORDER BY ipv4_;

SELECT '= 127.0.0.1', ipv4_ FROM ipv4_test
    WHERE ipv4_ = toIPv4('127.0.0.1')
    ORDER BY ipv4_;

-- TODO: Assert that invalid values can't be inserted into IPv4 column.

DROP TABLE IF EXISTS ipv4_test;


select 'euqality of IPv4-mapped IPv6 value and IPv4 promoted to IPv6 with function:', toIPv6('::ffff:127.0.0.1') = IPv4ToIPv6(toIPv4('127.0.0.1'));


DROP TABLE IF EXISTS ipv6_test;

-- Only valid values for IPv6
CREATE TABLE ipv6_test (ipv6_ IPv6) ENGINE = Memory;

-- ipv6_ column shoud have type 'IPv6'
SHOW CREATE TABLE ipv6_test;

INSERT INTO ipv6_test VALUES ('::'), ('0:0:0:0:0:0:0:0'), ('FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF'), ('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), ('0000:0000:0000:0000:0000:FFFF:C1FC:110A'), ('::ffff:127.0.0.1'), ('::ffff:8.8.8.8');

SELECT ipv6_, hex(ipv6_) FROM ipv6_test ORDER BY ipv6_;

SELECT '< 127.0.0.1', ipv6_ FROM ipv6_test
    WHERE ipv6_ < IPv4ToIPv6(toIPv4('127.0.0.1'))
    ORDER BY ipv6_;

SELECT '> 127.0.0.1', ipv6_ FROM ipv6_test
    WHERE ipv6_ > IPv4ToIPv6(toIPv4('127.0.0.1'))
    ORDER BY ipv6_;

SELECT '= 127.0.0.1', ipv6_ FROM ipv6_test
    WHERE ipv6_ = IPv4ToIPv6(toIPv4('127.0.0.1'))
    ORDER BY ipv6_;

-- TODO: Assert that invalid values can't be inserted into IPv6 column.

DROP TABLE IF EXISTS ipv6_test;

SELECT '0.0.0.0 is ipv4 string:                                 ', isIPv4String('0.0.0.0');
SELECT '255.255.255.255 is ipv4 string:                         ', isIPv4String('255.255.255.255');
SELECT '192.168.0.91 is ipv4 string:                            ', isIPv4String('192.168.0.91');
SELECT '127.0.0.1 is ipv4 string:                               ', isIPv4String('127.0.0.1');
SELECT '8.8.8.8 is ipv4 string:                                 ', isIPv4String('8.8.8.8');
SELECT 'hello is ipv4 string:                                   ', isIPv4String('hello');
SELECT '0:0:0:0:0:0:0:0 is ipv4 string:                         ', isIPv4String('0:0:0:0:0:0:0:0');
SELECT '0000:0000:0000:0000:0000:FFFF:C1FC:110A is ipv4 string: ', isIPv4String('0000:0000:0000:0000:0000:FFFF:C1FC:110A');
SELECT 'FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF is ipv4 string: ', isIPv4String('FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF');
SELECT '::ffff:127.0.0.1 is ipv4 string:                        ', isIPv4String('::ffff:127.0.0.1');
SELECT '::ffff:8.8.8.8 is ipv4 string:                          ', isIPv4String('::ffff:8.8.8.8');
SELECT '2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D is ipv4 string: ', isIPv4String('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D');

SELECT '0.0.0.0 is ipv6 string:                                 ', isIPv6String('0.0.0.0');
SELECT '255.255.255.255 is ipv6 string:                         ', isIPv6String('255.255.255.255');
SELECT '192.168.0.91 is ipv6 string:                            ', isIPv6String('192.168.0.91');
SELECT '127.0.0.1 is ipv6 string:                               ', isIPv6String('127.0.0.1');
SELECT '8.8.8.8 is ipv6 string:                                 ', isIPv6String('8.8.8.8');
SELECT 'hello is ipv6 string:                                   ', isIPv6String('hello');
SELECT '0:0:0:0:0:0:0:0 is ipv6 string:                         ', isIPv6String('0:0:0:0:0:0:0:0');
SELECT '0000:0000:0000:0000:0000:FFFF:C1FC:110A is ipv6 string: ', isIPv6String('0000:0000:0000:0000:0000:FFFF:C1FC:110A');
SELECT 'FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF is ipv6 string: ', isIPv6String('FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF');
SELECT '::ffff:127.0.0.1 is ipv6 string:                        ', isIPv6String('::ffff:127.0.0.1');
SELECT '::ffff:8.8.8.8 is ipv6 string:                          ', isIPv6String('::ffff:8.8.8.8');
SELECT '2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D is ipv6 string: ', isIPv6String('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D');

-- IPV6 functions parse IPv4 addresses.

SELECT toIPv6('0.0.0.0');
SELECT toIPv6('127.0.0.1');
SELECT cutIPv6(IPv6StringToNum('127.0.0.1'), 0, 0);
SELECT toIPv6('127.0.0.' || toString(number)) FROM numbers(13);

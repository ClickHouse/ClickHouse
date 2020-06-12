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

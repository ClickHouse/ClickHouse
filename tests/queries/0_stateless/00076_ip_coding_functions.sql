SET cast_ipv4_ipv6_default_on_conversion_error = 1;

select IPv4StringToNum('') == 0;
select IPv4StringToNum(materialize('')) == 0;
select IPv4StringToNum('not an ip string') == 0;
select IPv4StringToNum(materialize('not an ip string')) == 0;
select IPv4StringToNum('127.0.0.1' as p) == (0x7f000001 as n), IPv4NumToString(n) == p;
select IPv4StringToNum(materialize('127.0.0.1') as p) == (materialize(0x7f000001) as n), IPv4NumToString(n) == p;
select IPv4NumToString(toUInt32(0)) == '0.0.0.0';
select IPv4NumToString(materialize(toUInt32(0))) == materialize('0.0.0.0');
select IPv4NumToString(toUInt32(0x7f000001)) == '127.0.0.1';
select IPv4NumToString(materialize(toUInt32(0x7f000001))) == materialize('127.0.0.1');

select IPv6NumToString(toFixedString('', 16)) == '::';
select IPv6NumToString(toFixedString(materialize(''), 16)) == materialize('::');
select IPv6NumToString(IPv6StringToNum('::ffff:127.0.0.1' as p) as n) == p;
select IPv6NumToString(IPv6StringToNum(materialize('::ffff:127.0.0.1') as p) as n) == p;
select IPv6NumToString(toFixedString(unhex('20010DB800000003000001FF0000002E'), 16)) == '2001:db8:0:3:0:1ff:0:2e';
select IPv6NumToString(toFixedString(unhex(materialize('20010DB800000003000001FF0000002E')), 16)) == materialize('2001:db8:0:3:0:1ff:0:2e');
select IPv6StringToNum('') == toFixedString(materialize(''), 16);
select IPv6StringToNum(materialize('')) == toFixedString(materialize(''), 16);
select IPv6StringToNum('not an ip string') == toFixedString(materialize(''), 16);
select IPv6StringToNum(materialize('not an ip string')) == toFixedString(materialize(''), 16);

/* IPv4ToIPv6 */

SELECT hex(IPv4ToIPv6(1297626935));

/* Тест с таблицей */

DROP TABLE IF EXISTS addresses;
CREATE TABLE addresses(addr UInt32) ENGINE = Memory;
INSERT INTO addresses(addr) VALUES (1297626935), (2130706433), (3254522122);
SELECT hex(IPv4ToIPv6(addr)) FROM addresses ORDER BY addr ASC;

/* cutIPv6 */

/*  Реальный IPv6-адрес */

SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 0);

SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 1, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 2, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 3, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 4, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 5, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 6, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 7, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 8, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 9, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 10, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 11, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 12, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 13, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 14, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 15, 0);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 16, 0);

SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 1);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 2);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 3);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 4);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 5);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 6);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 7);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 8);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 9);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 10);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 11);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 12);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 13);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 14);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 15);
SELECT cutIPv6(IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, 16);

/*  IPv4-mapped IPv6-адрес */

SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 0);

SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 1, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 2, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 3, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 4, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 5, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 6, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 7, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 8, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 9, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 10, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 11, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 12, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 13, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 14, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 15, 0);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 16, 0);

SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 1);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 2);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 3);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 4);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 5);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 6);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 7);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 8);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 9);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 10);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 11);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 12);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 13);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 14);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 15);
SELECT cutIPv6(toFixedString(unhex('00000000000000000000FFFFC1FC110A'), 16), 0, 16);

/* Тест с таблицами */

/*  Реальные IPv6-адреса */

DROP TABLE IF EXISTS addresses;
CREATE TABLE addresses(addr String) ENGINE = Memory;
INSERT INTO addresses(addr) VALUES ('20010DB8AC10FE01FEEDBABECAFEF00D'), ('20010DB8AC10FE01DEADC0DECAFED00D'), ('20010DB8AC10FE01ABADBABEFACEB00C');
SELECT cutIPv6(toFixedString(unhex(addr), 16), 3, 0) FROM addresses ORDER BY addr ASC;

/*  IPv4-mapped IPv6-адреса */

DROP TABLE IF EXISTS addresses;
CREATE TABLE addresses(addr String) ENGINE = Memory;
INSERT INTO addresses(addr) VALUES ('00000000000000000000FFFFC1FC110A'), ('00000000000000000000FFFF4D583737'), ('00000000000000000000FFFF7F000001');
SELECT cutIPv6(toFixedString(unhex(addr), 16), 0, 3) FROM addresses ORDER BY addr ASC;

DROP TABLE addresses;

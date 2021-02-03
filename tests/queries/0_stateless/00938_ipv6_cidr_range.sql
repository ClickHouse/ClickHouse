SELECT 'check invalid params';
SELECT IPv6CIDRToRange(1, 1); -- { serverError 43 }
SELECT IPv6CIDRToRange('1234', 1); -- { serverError 43 }
SELECT IPv6CIDRToRange(toFixedString('1234', 10), 1); -- { serverError 43 }
SELECT IPv6CIDRToRange(toFixedString('1234', 16), toUInt16(1)); -- { serverError 43 }

SELECT 'tests';

DROP TABLE IF EXISTS ipv6_range;
CREATE TABLE ipv6_range(ip IPv6, cidr UInt8) ENGINE = Memory;

INSERT INTO ipv6_range (ip, cidr) VALUES (IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 0), (IPv6StringToNum('2001:0db8:0000:85a3:ffff:ffff:ffff:ffff'), 32), (IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), 16), (IPv6StringToNum('2001:df8:0:85a3::ac1f:8001'), 32), (IPv6StringToNum('2001:0db8:85a3:85a3:0000:0000:ac1f:8001'), 16), (IPv6StringToNum('0000:0000:0000:0000:0000:0000:0000:0000'), 8), (IPv6StringToNum('ffff:0000:0000:0000:0000:0000:0000:0000'), 4);

WITH IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32) as ip_range SELECT COUNT(*) FROM ipv6_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 25) as ip_range SELECT COUNT(*) FROM ipv6_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 26) as ip_range SELECT COUNT(*) FROM ipv6_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 64) as ip_range SELECT COUNT(*) FROM ipv6_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 0) as ip_range SELECT COUNT(*) FROM ipv6_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

SELECT IPv6NumToString(ip), cidr, IPv6CIDRToRange(ip, cidr) FROM ipv6_range;

DROP TABLE ipv6_range;

SELECT IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 0);
SELECT IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 128);
SELECT IPv6CIDRToRange(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), 64);
SELECT IPv6CIDRToRange(IPv6StringToNum('0000:0000:0000:0000:0000:0000:0000:0000'), 8);
SELECT IPv6CIDRToRange(IPv6StringToNum('ffff:0000:0000:0000:0000:0000:0000:0000'), 4);
SELECT IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 128) = IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 200) ;

SELECT IPv6CIDRToRange(IPv6StringToNum('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), toUInt8(128 - number)) FROM numbers(2);

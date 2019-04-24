USE test;

DROP TABLE IF EXISTS ipv4_range;
CREATE TABLE ipv4_range(ip UInt32, cidr UInt8) ENGINE = Memory;

INSERT INTO ipv4_range (ip, cidr) VALUES (IPv4StringToNum('192.168.5.2'), 0), (IPv4StringToNum('192.168.5.20'), 32), (IPv4StringToNum('255.255.255.255'), 16), (IPv4StringToNum('192.142.32.2'), 32), (IPv4StringToNum('192.172.5.2'), 16), (IPv4StringToNum('0.0.0.0'), 8), (IPv4StringToNum('255.0.0.0'), 4);

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('192.168.0.0'), 8) as ip_range SELECT COUNT(*) FROM ipv4_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('192.168.0.0'), 13) as ip_range SELECT COUNT(*) FROM ipv4_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('192.168.0.0'), 16) as ip_range SELECT COUNT(*) FROM ipv4_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('192.168.0.0'), 0) as ip_range SELECT COUNT(*) FROM ipv4_range WHERE ip BETWEEN tupleElement(ip_range, 1) AND tupleElement(ip_range, 2);

WITH IPv4CIDRtoIPv4Range(ip, cidr) as ip_range SELECT IPv4NumToString(ip), cidr, IPv4NumToString(tupleElement(ip_range, 1)), IPv4NumToString(tupleElement(ip_range, 2)) FROM ipv4_range;

DROP TABLE ipv4_range;

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('192.168.5.2'), 0) as ip_range SELECT IPv4NumToString(tupleElement(ip_range, 1)) as lower_range, IPv4NumToString(tupleElement(ip_range, 2)) as higher_range;

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('192.168.5.2'), 32) as ip_range SELECT IPv4NumToString(tupleElement(ip_range, 1)) as lower_range, IPv4NumToString(tupleElement(ip_range, 2)) as higher_range;

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('255.255.255.255'), 8) as ip_range SELECT IPv4NumToString(tupleElement(ip_range, 1)) as lower_range, IPv4NumToString(tupleElement(ip_range, 2)) as higher_range;

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('0.0.0.0'), 8) as ip_range SELECT IPv4NumToString(tupleElement(ip_range, 1)) as lower_range, IPv4NumToString(tupleElement(ip_range, 2)) as higher_range;

WITH IPv4CIDRtoIPv4Range(IPv4StringToNum('255.0.0.0'), 4) as ip_range SELECT IPv4NumToString(tupleElement(ip_range, 1)) as lower_range, IPv4NumToString(tupleElement(ip_range, 2)) as higher_range;


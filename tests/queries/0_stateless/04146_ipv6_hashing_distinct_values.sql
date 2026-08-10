-- Tags: no-fasttest
-- Reason: SHA256 needs OpenSSL, which the fast-test build does not link.

-- Distinct IPv6 values must hash to distinct digests.
SELECT count(DISTINCT SHA256(addr))
FROM (SELECT toIPv6(arrayJoin(['::1', '::2', 'fe80::1', '2001:db8::1'])) AS addr);

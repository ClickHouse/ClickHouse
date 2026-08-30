-- Tags: no-fasttest
-- Reason: SHA256 needs OpenSSL, which the fast-test build does not link.

-- Each row must be hashed from its own address, so the address-to-digest mapping is pinned
-- per row: a permuted read would keep the digests distinct but pair them up wrongly.
SELECT addr, hex(SHA256(addr))
FROM (SELECT toIPv6(arrayJoin(['::1', '::2', 'fe80::1', '2001:db8::1'])) AS addr)
ORDER BY addr;

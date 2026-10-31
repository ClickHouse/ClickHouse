SELECT domainRFC('http://[2001:db8::1]:80') FORMAT CSV;
SELECT domainRFC('[2001:db8::1]:80') FORMAT CSV;
SELECT domainRFC('[::200]:80') FORMAT CSV;
SELECT domainRFC('[2001:db8::1]') FORMAT CSV;
SELECT domainRFC('http://user@[2001:db8::1]:80') FORMAT CSV;
SELECT domainRFC('http://user:password@[2001:db8::1]:80') FORMAT CSV;
SELECT domainRFC('user@[2001:db8::1]:80') FORMAT CSV;
-- Nothing may follow the closing bracket of an IP-literal except a delimiter or end of input.
SELECT domainRFC('http://[2001:db8::1]evil.com') FORMAT CSV;
SELECT domainRFC('http://user@[2001:db8::1]evil.com') FORMAT CSV;
-- A colon or dot in the userinfo must not leak into validation of the host that follows it.
SELECT domainRFC('http://user:password@[20[01:db8::1]:80') FORMAT CSV;
SELECT domainRFC('http://user.name@[2001:db8::1') FORMAT CSV;
-- The bracket contents must be a real IPv6 address, not just a bracket-balanced string.
SELECT domainRFC('http://[2001db81]:80') FORMAT CSV;
SELECT domainRFC('http://user@[2001db81]:80') FORMAT CSV;
-- A mixed IPv6/IPv4 tail literal is a valid IPv6 address and its dot must not be rejected.
SELECT domainRFC('http://[::ffff:192.0.2.128]:80') FORMAT CSV;
SELECT domainRFC('http://user@[::ffff:192.0.2.128]:80') FORMAT CSV;
-- userinfo may contain more than one ':' (RFC 3986); it must not be mistaken for the host:port separator.
SELECT domainRFC('http://user:pass:word@[2001:db8::1]:80') FORMAT CSV;
-- userinfo cannot legally contain a raw '@' (RFC 3986); a second '@' makes the authority
-- unparseable and must be rejected.
SELECT domainRFC('http://user@paypal.com@[2001:db8::1]:80') FORMAT CSV;
-- Same as above, but with an explicit port before the second '@', and with plain, IPv6, and IPv4 hosts.
SELECT domainRFC('http://user@paypal.com:80@evil.com/') FORMAT CSV;
SELECT domainRFC('http://user@[::1]:80@evil.com/') FORMAT CSV;
SELECT domainRFC('http://user@192.168.1.1:80@evil.com/') FORMAT CSV;
-- A bracketed host that already closed before '@' would have to become part of userinfo, but raw
-- '[' / ']' can never legally appear there.
SELECT domainRFC('http://[::1]:80@[::2]') FORMAT CSV;
SELECT domainRFC('http://[::1]:80@evil.com') FORMAT CSV;
-- Does not conform to the IPv6 format.
SELECT domainRFC('[2001db81]:80') FORMAT CSV;
SELECT domainRFC('[20[01:db8::1]:80') FORMAT CSV;
SELECT domainRFC('[20[01:db]8::1]:80') FORMAT CSV;
SELECT domainRFC('[2001:db8::1') FORMAT CSV;
SELECT domainRFC('2001:db8::1]:80') FORMAT CSV;
SELECT domainRFC('[2001db81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db.81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db/81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db?81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db#81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db@81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db;81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db=81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db&81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db~81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db%81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db<81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db>81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db{81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db}81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db|81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db\81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db^81]:80') FORMAT CSV;
SELECT domainRFC('[2001::db 81]:80') FORMAT CSV;
SELECT domainRFC('[[]:80') FORMAT CSV;
SELECT domainRFC('[]]:80') FORMAT CSV;
SELECT domainRFC('[]:80') FORMAT CSV;
SELECT domainRFC('[ ]:80') FORMAT CSV;

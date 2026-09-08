-- userinfo cannot legally contain a raw '@' (RFC 3986: userinfo = *( unreserved / pct-encoded / sub-delims / ":" )).
-- A second (or later) '@' means everything up to it was actually userinfo, so the real host is
-- whatever follows the LAST '@', not the fake host-looking segment in between - otherwise a URL
-- like http://user@paypal.com@evil.com/ could be mistaken for having host paypal.com instead of
-- the real host evil.com.
SELECT domainRFC('http://user@paypal.com@evil.com/') FORMAT CSV;
SELECT domainRFC('http://paypal.com@evil.com/') FORMAT CSV;
SELECT domainRFC('http://user@paypal.com@evil.com:8080/') FORMAT CSV;
SELECT domainRFC('http://a@b@c.com/') FORMAT CSV;
SELECT domainRFC('http://x@y@z@w.com/') FORMAT CSV;

SELECT portRFC('http://user@paypal.com@evil.com:8080/') FORMAT CSV;
SELECT portRFC('http://user@paypal.com@evil.com/', toUInt16(443)) FORMAT CSV;

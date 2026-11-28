-- userinfo cannot legally contain a raw '@' (RFC 3986: userinfo = *( unreserved / pct-encoded / sub-delims / ":" )).
-- A second (or later) '@' makes the authority unparseable, so it must be rejected (empty / the
-- default) rather than guessing which segment is the real host - otherwise a URL like
-- http://user@paypal.com@evil.com/ could be mistaken for having host paypal.com.
SELECT domainRFC('http://user@paypal.com@evil.com/') FORMAT CSV;
SELECT domainRFC('http://user@paypal.com@evil.com:8080/') FORMAT CSV;
SELECT domainRFC('http://a@b@c.com/') FORMAT CSV;
SELECT domainRFC('http://x@y@z@w.com/') FORMAT CSV;

SELECT portRFC('http://user@paypal.com@evil.com:8080/') FORMAT CSV;
SELECT portRFC('http://user@paypal.com@evil.com/', toUInt16(443)) FORMAT CSV;

-- a single '@' is unaffected and still resolves to the host after it
SELECT domainRFC('http://paypal.com@evil.com/') FORMAT CSV;

-- a host cannot legally start with a dot
SELECT domainRFC('http://user@.com/') FORMAT CSV;
SELECT domainRFC('.com') FORMAT CSV;
SELECT domainRFC('http://.com/') FORMAT CSV;
SELECT domain('http://user@.com/') FORMAT CSV;
SELECT domain('.com') FORMAT CSV;
SELECT domain('http://.com/') FORMAT CSV;
-- Two (or more) leading dots must be rejected too, not just a host that is exactly ".something".
SELECT domainRFC('http://..com/') FORMAT CSV;
SELECT domainRFC('http://user@..com/') FORMAT CSV;
SELECT domain('http://..com/') FORMAT CSV;

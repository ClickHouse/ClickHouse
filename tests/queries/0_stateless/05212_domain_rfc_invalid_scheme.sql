-- RFC 3986, 3.1: scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ); '[' is not a legal scheme
-- character. The scheme-detection pre-scan used to allow it anyway, so a string like "a[://foo.com"
-- was treated as if "a[" were a valid scheme, and "foo.com" was parsed as the host.
SELECT domainRFC('a[://foo.com') FORMAT CSV;
SELECT portRFC('a[://foo.com:80', toUInt16(443)) FORMAT CSV;

-- A legitimate bracketed IP-literal host is unaffected: '[' never needs to be treated as part of a
-- scheme, since it either starts the authority directly (never reached by the scheme scan) or
-- follows the scheme's own '://', which the scan already recognizes first.
SELECT domainRFC('http://[2001:db8::1]:80') FORMAT CSV;

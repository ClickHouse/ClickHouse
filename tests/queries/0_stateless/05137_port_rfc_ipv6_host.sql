-- An IP-literal host (RFC 3986, 3.2.2) is followed by `]` before the `:` of the port.
SELECT portRFC('http://[2001:db8::1]:8080/');
SELECT portRFC('http://[2001:db8::1]:8080/', 443);
SELECT portRFC('https://[::1]:9440/path');
SELECT portRFC('//[2001:db8::1]:8080/');
SELECT portRFC('[2001:db8::1]:8080');
SELECT portRFC('http://[2001:db8::1]:8080');

-- The userinfo of an authority may precede an IP-literal host (RFC 3986, 3.2).
SELECT portRFC('http://user:password@[2001:db8::1]:8080/');
SELECT portRFC('http://user@[::1]:9440/path', 443);
SELECT domainRFC('http://user:password@[2001:db8::1]:8080/');
SELECT portRFC('http://user:password@[2001:db8::1]/', 443);

-- No port: the default is returned.
SELECT portRFC('http://[2001:db8::1]/');
SELECT portRFC('http://[2001:db8::1]/', 443);

-- The host of the same URL, for comparison.
SELECT domainRFC('http://[2001:db8::1]:8080/');

-- `port` does not parse an IP-literal host at all, so it has no port to return.
SELECT port('http://[2001:db8::1]:8080/', 443);
SELECT domain('http://[2001:db8::1]:8080/');

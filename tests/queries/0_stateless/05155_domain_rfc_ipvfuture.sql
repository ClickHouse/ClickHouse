-- RFC 3986, 3.2.2: IP-literal = "[" ( IPv6address / IPvFuture ) "]", IPvFuture = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
SELECT domainRFC('http://[v1.a]:80/') FORMAT CSV;
SELECT domainRFC('http://user@[v1.a]:80/') FORMAT CSV;
SELECT domainRFC('//[v1.a]:80/') FORMAT CSV;
SELECT domainRFC('http://[vA.fe80::1]:80/') FORMAT CSV;
SELECT portRFC('http://[v1.a]:80/') FORMAT CSV;
SELECT portRFC('http://[v1.a]/', toUInt16(443)) FORMAT CSV;

-- Malformed IPvFuture must still be rejected.
SELECT domainRFC('http://[v.a]:80/') FORMAT CSV; -- missing hex version digit
SELECT domainRFC('http://[v1.]:80/') FORMAT CSV; -- missing address part
SELECT domainRFC('http://[v1a]:80/') FORMAT CSV; -- missing '.'
SELECT domainRFC('http://[1.a]:80/') FORMAT CSV; -- does not start with 'v'
SELECT domainRFC('http://[v1.a b]:80/') FORMAT CSV; -- space not allowed

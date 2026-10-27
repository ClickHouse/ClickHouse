-- Once a ':' is seen, a '.' afterward belongs to the port (or whatever junk follows it), not to
-- the host, and must not make a single-label host look like it has a valid dot of its own.
SELECT domainRFC('http://user@foo:80.bar/') FORMAT CSV;
SELECT domainRFC('http://foo:80.bar/') FORMAT CSV;
SELECT domainWithoutWWWRFC('http://user@foo:80.bar/') FORMAT CSV;

-- Control: a dot within the host itself, before the port, is unaffected.
SELECT domainRFC('http://user@foo.com:80/') FORMAT CSV;

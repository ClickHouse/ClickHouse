-- Test for empty and short strings in domain functions to catch use-of-uninitialized-value errors (MSan)

SELECT domainRFC('');
SELECT domainRFC('a');
SELECT domainRFC('/');
SELECT domainRFC('//');

SELECT domainWithoutWWWRFC('');
SELECT domainWithoutWWWRFC('a');
SELECT domainWithoutWWWRFC('/');
SELECT domainWithoutWWWRFC('//');

SELECT domain('');
SELECT domain('a');
SELECT domain('/');
SELECT domain('//');

SELECT domainWithoutWWW('');
SELECT domainWithoutWWW('a');
SELECT domainWithoutWWW('/');
SELECT domainWithoutWWW('//');

-- An authority that ends exactly at the scheme prefix leaves the host scan positioned at the end of
-- the input. `ColumnString` is not zero-terminated, so the byte one past the input is uninitialised
-- heap. The returned host is empty either way, so only an MSan build can observe the read.
SELECT domainRFC('http://');
SELECT domainRFC('aa://');
SELECT domainRFC('a://');
SELECT domainWithoutWWWRFC('http://');
SELECT topLevelDomainRFC('http://');
SELECT portRFC('http://');
SELECT firstSignificantSubdomainRFC('http://');
SELECT cutToFirstSignificantSubdomainRFC('http://');
SELECT domainRFC(materialize('http://'));

SELECT domainRFC('http://[');
SELECT domainRFC('http://user@[');
SELECT domainRFC('http://foo:');
SELECT domainRFC('http://user@foo:');
SELECT domainRFC('http://[2001:db8::1');
SELECT domainRFC('http://[2001:db8::1]');
SELECT domainRFC('http://[2001:db8::1]:');
SELECT domainRFC('http://[v');
SELECT domainRFC('http://[v1');
SELECT domainRFC('http://[v1.');
SELECT domainRFC('http://[v1.a');
SELECT domainRFC('http://[v1.a]');
SELECT domainRFC('http://user@');
SELECT domainRFC('http://user@[2001:db8::1]:80@');
SELECT domainRFC('http://[]');
SELECT domainRFC('http://user@[]');

SELECT portRFC('http://[');
SELECT portRFC('http://foo:');
SELECT portRFC('http://[2001:db8::1]');
SELECT portRFC('http://[2001:db8::1]:');
SELECT portRFC('http://[v1.a]');

SELECT firstSignificantSubdomainRFC('http://[v1.a]');
SELECT cutToFirstSignificantSubdomainRFC('http://[v1.a]');
SELECT topLevelDomainRFC('http://[v1.a]');
SELECT firstSignificantSubdomainRFC('http://[');

SELECT domainRFC(materialize('http://[v1.a]'));

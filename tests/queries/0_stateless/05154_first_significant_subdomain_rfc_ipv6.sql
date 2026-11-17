-- getURLHostRFC() returns a bracketed IP-literal host without its brackets, so it may contain ':',
-- which a reg-name host never does. An IP address is not a DNS name, so it has no significant
-- subdomain to extract, and must not be treated as one.
SELECT firstSignificantSubdomainRFC('http://[::ffff:192.0.2.128]:80');
SELECT firstSignificantSubdomainRFC('http://[2001:db8::1]:80');
SELECT firstSignificantSubdomainRFC('http://user@[::ffff:192.0.2.128]:80');

SELECT cutToFirstSignificantSubdomainRFC('http://[::ffff:192.0.2.128]:80');
SELECT cutToFirstSignificantSubdomainRFC('http://[2001:db8::1]:80');
SELECT cutToFirstSignificantSubdomainWithWWWRFC('http://[::ffff:192.0.2.128]:80');

SELECT firstSignificantSubdomainCustomRFC('http://[::ffff:192.0.2.128]:80', 'public_suffix_list');
SELECT cutToFirstSignificantSubdomainCustomRFC('http://[::ffff:192.0.2.128]:80', 'public_suffix_list');

-- Detecting the IP-literal from the surrounding brackets (rather than from a ':' in the host
-- itself) also catches an IPvFuture host, which contains none.
SELECT firstSignificantSubdomainRFC('http://[v1.a]:80/');
SELECT cutToFirstSignificantSubdomainRFC('http://[v1.a]:80/');

-- A plain (non-RFC) reg-name host is unaffected; these still work as before.
SELECT firstSignificantSubdomainRFC('http://www.example.com/');
SELECT cutToFirstSignificantSubdomainRFC('http://www.example.com/');

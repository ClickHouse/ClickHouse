-- `getURLHostRFC` rejected every bracketed IP-literal that contains a dot, so an `IPv6address` ending
-- in an `IPv4address` - a form RFC 3986 allows (`ls32 = ( h16 ":" h16 ) / IPv4address`) - had no host
-- and no port. A dot that is not part of such an address is still not allowed in an IP-literal.

SELECT domainRFC('http://[::ffff:192.168.0.1]:8080/'), portRFC('http://[::ffff:192.168.0.1]:8080/');
SELECT domainRFC('http://[64:ff9b::192.0.2.33]/'), portRFC('http://[64:ff9b::192.0.2.33]/');
SELECT domainRFC('http://[::ffff:192.168.0.1]/path?a=1'), domainWithoutWWWRFC('http://[::ffff:192.168.0.1]:8080/');
SELECT domainRFC('http://user:password@[::ffff:192.168.0.1]:8080/'), portRFC('http://user:password@[::ffff:192.168.0.1]:8080/');

SELECT 'hex-only literals are unchanged';
SELECT domainRFC('http://[2001:db8::1]:8080/'), portRFC('http://[2001:db8::1]:8080/');

SELECT 'a dotted literal that is not an IPv6 address is not a host';
SELECT domainRFC('http://[2001::db.81]:80/') = '', portRFC('http://[2001::db.81]:80/');
SELECT domainRFC('http://[192.168.0.1.2]/') = '';

SELECT 'and the plain (non-RFC) functions are unchanged';
SELECT domain('http://[::ffff:192.168.0.1]:8080/') = '', port('http://[::ffff:192.168.0.1]:8080/');

SELECT '# Invocation with constants';

SELECT isIPAddressInRange('127.0.0.1', '127.0.0.0/8');
SELECT isIPAddressInRange('128.0.0.1', '127.0.0.0/8');

SELECT isIPAddressInRange('ffff::1', 'ffff::/16');
SELECT isIPAddressInRange('fffe::1', 'ffff::/16');

SELECT '# Invocation with non-constant addresses';

WITH arrayJoin(['192.168.99.255', '192.168.100.1', '192.168.103.255', '192.168.104.0']) as addr, '192.168.100.0/22' as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);
WITH arrayJoin(['::192.168.99.255', '::192.168.100.1', '::192.168.103.255', '::192.168.104.0']) as addr, '::192.168.100.0/118' as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);

SELECT '# Invocation with non-constant prefixes';

WITH '192.168.100.1' as addr, arrayJoin(['192.168.100.0/22', '192.168.100.0/24', '192.168.100.0/32']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);
WITH '::192.168.100.1' as addr, arrayJoin(['::192.168.100.0/118', '::192.168.100.0/120', '::192.168.100.0/128']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);

SELECT '# Invocation with non-constants';

WITH arrayJoin(['192.168.100.1', '192.168.103.255']) as addr, arrayJoin(['192.168.100.0/22', '192.168.100.0/24']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);
WITH arrayJoin(['::192.168.100.1', '::192.168.103.255']) as addr, arrayJoin(['::192.168.100.0/118', '::192.168.100.0/120']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);

SELECT '# Mismatching IP versions is not an error.';

SELECT isIPAddressInRange('127.0.0.1', 'ffff::/16');
SELECT isIPAddressInRange('127.0.0.1', '::127.0.0.1/128');
SELECT isIPAddressInRange('::1', '127.0.0.0/8');
SELECT isIPAddressInRange('::127.0.0.1', '127.0.0.1/32');

SELECT '# Unparsable arguments';

SELECT isIPAddressInRange('unparsable', '127.0.0.0/8'); -- { serverError 6 }
SELECT isIPAddressInRange('127.0.0.1', 'unparsable'); -- { serverError 6 }

SELECT '# Wrong argument types';

SELECT isIPAddressInRange(100, '127.0.0.0/8'); -- { serverError 43 }
SELECT isIPAddressInRange(NULL, '127.0.0.0/8'); -- { serverError 43 }
SELECT isIPAddressInRange(CAST(NULL, 'Nullable(String)'), '127.0.0.0/8'); -- { serverError 43 }
SELECT isIPAddressInRange('127.0.0.1', 100); -- { serverError 43 }
SELECT isIPAddressInRange(100, NULL); -- { serverError 43 }
WITH arrayJoin([NULL, NULL, NULL, NULL]) AS prefix SELECT isIPAddressInRange([NULL, NULL, 0, 255, 0], prefix); -- { serverError 43 }

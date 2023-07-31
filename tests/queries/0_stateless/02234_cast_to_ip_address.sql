SELECT 'IPv4 functions';

SELECT IPv4StringToNum('test'); --{serverError 441}
SELECT IPv4StringToNumOrDefault('test');
SELECT IPv4StringToNumOrNull('test');

SELECT IPv4StringToNum('127.0.0.1');
SELECT IPv4StringToNumOrDefault('127.0.0.1');
SELECT IPv4StringToNumOrNull('127.0.0.1');

SELECT '--';

SELECT toIPv4('test'); --{serverError 441}
SELECT toIPv4OrDefault('test');
SELECT toIPv4OrNull('test');

SELECT toIPv4('127.0.0.1');
SELECT toIPv4OrDefault('127.0.0.1');
SELECT toIPv4OrNull('127.0.0.1');

SELECT '--';

SELECT cast('test' , 'IPv4'); --{serverError 441}
SELECT cast('127.0.0.1' , 'IPv4');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

SELECT IPv4StringToNum('test');
SELECT toIPv4('test');
SELECT IPv4StringToNum('');
SELECT toIPv4('');
SELECT cast('test' , 'IPv4');
SELECT cast('' , 'IPv4');

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

SELECT 'IPv6 functions';

SELECT IPv6StringToNum('test'); --{serverError 441}
SELECT IPv6StringToNumOrDefault('test');
SELECT IPv6StringToNumOrNull('test');

SELECT IPv6StringToNum('::ffff:127.0.0.1');
SELECT IPv6StringToNumOrDefault('::ffff:127.0.0.1');
SELECT IPv6StringToNumOrNull('::ffff:127.0.0.1');

SELECT '--';

SELECT toIPv6('test'); --{serverError 441}
SELECT toIPv6OrDefault('test');
SELECT toIPv6OrNull('test');

SELECT toIPv6('::ffff:127.0.0.1');
SELECT toIPv6OrDefault('::ffff:127.0.0.1');
SELECT toIPv6OrNull('::ffff:127.0.0.1');

SELECT '--';

SELECT cast('test' , 'IPv6'); --{serverError 441}
SELECT cast('::ffff:127.0.0.1', 'IPv6');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

SELECT IPv6StringToNum('test');
SELECT toIPv6('test');
SELECT IPv6StringToNum('');
SELECT toIPv6('');
SELECT cast('test' , 'IPv6');
SELECT cast('' , 'IPv6');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

SELECT toFixedString('::ffff:127.0.0.1', 16) as value, cast(value, 'IPv6'), toIPv6(value);
SELECT toFixedString('::1', 5) as value, cast(value, 'IPv6'), toIPv6(value);
SELECT toFixedString('', 16) as value, cast(value, 'IPv6'); --{serverError 441}
SELECT toFixedString('', 16) as value, toIPv6(value); --{serverError 441}

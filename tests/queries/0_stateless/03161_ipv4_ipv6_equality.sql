-- Equal
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
SELECT toIPv6('::ffff:127.0.0.1') = toIPv4('127.0.0.1');

-- Not equal
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.2');
SELECT toIPv4('127.0.0.2') = toIPv6('::ffff:127.0.0.1');
SELECT toIPv6('::ffff:127.0.0.1') = toIPv4('127.0.0.2');
SELECT toIPv6('::ffff:127.0.0.2') = toIPv4('127.0.0.1');
SELECT toIPv4('127.0.0.1') = toIPv6('::ffef:127.0.0.1');
SELECT toIPv6('::ffef:127.0.0.1') = toIPv4('127.0.0.1');
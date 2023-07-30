SELECT empty(toIPv6('::'));
SELECT notEmpty(toIPv6('::'));
SELECT empty(toIPv6('::1'));
SELECT notEmpty(toIPv6('::1'));

SELECT empty(toIPv4('0.0.0.0'));
SELECT notEmpty(toIPv4('0.0.0.0'));
SELECT empty(toIPv4('127.0.0.1'));
SELECT notEmpty(toIPv4('127.0.0.1'));

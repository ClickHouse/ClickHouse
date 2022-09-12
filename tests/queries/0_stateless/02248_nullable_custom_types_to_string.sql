-- Tags: no-backward-compatibility-check:22.3.4.44
select toString(toNullable(true));
select toString(CAST(NULL, 'Nullable(Bool)'));
select toString(toNullable(toIPv4('0.0.0.0')));
select toString(CAST(NULL, 'Nullable(IPv4)'));
select toString(toNullable(toIPv6('::ffff:127.0.0.1')));
select toString(CAST(NULL, 'Nullable(IPv6)'));

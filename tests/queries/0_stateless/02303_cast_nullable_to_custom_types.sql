select CAST(CAST(NULL, 'Nullable(String)'), 'Nullable(Bool)');
select CAST(CAST(NULL, 'Nullable(String)'), 'Nullable(IPv4)');
select CAST(CAST(NULL, 'Nullable(String)'), 'Nullable(IPv6)');

select toBool(CAST(NULL, 'Nullable(String)'));
select toIPv4(CAST(NULL, 'Nullable(String)'));
select IPv4StringToNum(CAST(NULL, 'Nullable(String)'));
select toIPv6(CAST(NULL, 'Nullable(String)'));
select IPv6StringToNum(CAST(NULL, 'Nullable(String)'));

select CAST(number % 2 ? 'true' : NULL, 'Nullable(Bool)') from numbers(2);
select CAST(number % 2 ? '0.0.0.0' : NULL, 'Nullable(IPv4)') from numbers(2);
select CAST(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL, 'Nullable(IPv6)') from numbers(2);

select toBool(number % 2 ? 'true' : NULL) from numbers(2);
select toIPv4(number % 2 ? '0.0.0.0' : NULL) from numbers(2);
select toIPv4OrDefault(number % 2 ? '' : NULL) from numbers(2);
select toIPv4OrNull(number % 2 ? '' : NULL) from numbers(2);
select IPv4StringToNum(number % 2 ? '0.0.0.0' : NULL) from numbers(2);
select toIPv6(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL) from numbers(2);
select toIPv6OrDefault(number % 2 ? '' : NULL) from numbers(2);
select toIPv6OrNull(number % 2 ? '' : NULL) from numbers(2);
select IPv6StringToNum(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL) from numbers(2);

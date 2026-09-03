select IPv6NumToString(IPv6StringToNumOrDefault(x)) from system.one array join ['24', '5.123.234'] as x;
select IPv4NumToString(IPv4StringToNumOrDefault(x)) from system.one array join ['24', '5.123.234'] as x;
select IPv4NumToString(IPv4StringToNumOrDefault('111.111111.'));

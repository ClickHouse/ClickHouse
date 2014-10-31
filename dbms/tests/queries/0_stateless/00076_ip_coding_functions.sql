select IPv4StringToNum('') == 0;
select IPv4StringToNum(materialize('')) == 0;
select IPv4StringToNum('not an ip string') == 0;
select IPv4StringToNum(materialize('not an ip string')) == 0;
select IPv4StringToNum('127.0.0.1' as p) == (0x7f000001 as n), IPv4NumToString(n) == p;
select IPv4StringToNum(materialize('127.0.0.1') as p) == (materialize(0x7f000001) as n), IPv4NumToString(n) == p;
select IPv4NumToString(toUInt32(0)) == '0.0.0.0';
select IPv4NumToString(materialize(toUInt32(0))) == materialize('0.0.0.0');

select IPv6NumToString(toFixedString('', 16)) == '::';
select IPv6NumToString(toFixedString(materialize(''), 16)) == materialize('::');
select IPv6NumToString(IPv6StringToNum('::ffff:127.0.0.1' as p) as n) == p;
select IPv6NumToString(IPv6StringToNum(materialize('::ffff:127.0.0.1') as p) as n) == p;
select IPv6NumToString(toFixedString(unhex('20010DB800000003000001FF0000002E'), 16)) == '2001:db8:0:3:0:1ff:0:2e';
select IPv6NumToString(toFixedString(unhex(materialize('20010DB800000003000001FF0000002E')), 16)) == materialize('2001:db8:0:3:0:1ff:0:2e');
select IPv6StringToNum('') == toFixedString(materialize(''), 16);
select IPv6StringToNum(materialize('')) == toFixedString(materialize(''), 16);
select IPv6StringToNum('not an ip string') == toFixedString(materialize(''), 16);
select IPv6StringToNum(materialize('not an ip string')) == toFixedString(materialize(''), 16);

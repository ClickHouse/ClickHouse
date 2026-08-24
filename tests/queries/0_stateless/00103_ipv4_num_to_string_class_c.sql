select IPv4NumToStringClassC(toUInt32(0)) = '0.0.0.xxx';
select IPv4NumToStringClassC(0x7f000001) = '127.0.0.xxx';
select sum(IPv4NumToStringClassC(materialize(toUInt32(0))) = '0.0.0.xxx') = count() from system.one array join range(1024) as n;
select sum(IPv4NumToStringClassC(materialize(0x7f000001)) = '127.0.0.xxx') = count() from system.one array join range(1024) as n;

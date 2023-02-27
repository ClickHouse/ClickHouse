drop table if exists ipv6_test26473;

CREATE TABLE ipv6_test26473 (
`ip` String,
`ipv6` IPv6 MATERIALIZED toIPv6(ip),
`is_ipv6` Boolean   MATERIALIZED isIPv6String(ip),
`cblock` IPv6   MATERIALIZED cutIPv6(ipv6, 10, 1),
`cblock1` IPv6  MATERIALIZED toIPv6(cutIPv6(ipv6, 10, 1))  
)
ENGINE = Memory;

insert into ipv6_test26473 values ('2600:1011:b104:a86f:2832:b9c6:6d45:237b');

select ip, ipv6,cblock, cblock1,is_ipv6, cutIPv6(ipv6, 10, 1) from ipv6_test26473;

drop table ipv6_test26473;

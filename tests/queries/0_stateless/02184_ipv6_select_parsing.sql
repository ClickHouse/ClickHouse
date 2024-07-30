drop table if exists ips_v6;
create table ips_v6(i IPv6) Engine=Memory;

INSERT INTO ips_v6 SELECT toIPv6('::ffff:127.0.0.1');

INSERT INTO ips_v6 values       ('::ffff:127.0.0.1');

INSERT INTO ips_v6     FORMAT TSV ::ffff:127.0.0.1

INSERT INTO ips_v6 SELECT       ('::ffff:127.0.0.1');

SELECT * FROM ips_v6;
drop table ips_v6;

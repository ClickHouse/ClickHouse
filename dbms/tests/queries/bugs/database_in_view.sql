DROP TABLE IF EXISTS test.whoami;
DROP TABLE IF EXISTS test.tellme;
DROP TABLE IF EXISTS test.tellme_nested;

use test;
create view whoami as select 1 as n;
create view tellme as select * from whoami;
create view tellme_nested as select * from (select * from whoami);
select * from tellme;
select * from tellme_nested;

use default;
select * from test.tellme;
select * from test.tellme_nested;

DROP TABLE test.whoami;
DROP TABLE test.tellme;
DROP TABLE test.tellme_nested;

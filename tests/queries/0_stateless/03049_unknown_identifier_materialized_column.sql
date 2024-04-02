-- https://github.com/ClickHouse/ClickHouse/issues/54317
DROP DATABASE IF EXISTS 03049_database;
DROP TABLE IF EXISTS 03049_database.l;
DROP TABLE IF EXISTS 03049_database.r;

CREATE DATABASE 03049_database;
USE 03049_database;

CREATE TABLE l (y String) Engine Memory;
CREATE TABLE r (d Date, y String, ty UInt16 MATERIALIZED toYear(d)) Engine Memory;
select * from l L left join r R on  L.y = R.y  where R.ty >= 2019;
select * from l left join r  on  l.y = r.y  where r.ty >= 2019;
select * from 03049_database.l left join 03049_database.r  on  l.y = r.y  where r.ty >= 2019;

DROP DATABASE IF EXISTS 03049_database;
DROP TABLE IF EXISTS 03049_database.l;
DROP TABLE IF EXISTS 03049_database.r;

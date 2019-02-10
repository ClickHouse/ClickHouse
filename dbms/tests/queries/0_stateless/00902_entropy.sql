CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
    vals String
) ENGINE = Memory;

insert into test.defaults values ('ba'), ('aa'), ('ba'), ('b'), ('ba'), ('aa');
select val < 1.5 and val > 1.459 from (select entropy(vals) as val from test.defaults);


CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
     vals UInt64
) ENGINE = Memory;
insert into test.defaults values (0), (0), (1), (0), (0), (0), (1), (2), (3), (5), (3), (1), (1), (4), (5), (2)
select val < 2.4 and val > 2.3393 from (select entropy(vals) as val from test.defaults);


CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
    vals UInt32
) ENGINE = Memory;
insert into test.defaults values (0), (0), (1), (0), (0), (0), (1), (2), (3), (5), (3), (1), (1), (4), (5), (2)
select val < 2.4 and val > 2.3393 from (select entropy(vals) as val from test.defaults);


CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
    vals Int32
) ENGINE = Memory;
insert into test.defaults values (0), (0), (-1), (0), (0), (0), (-1), (2), (3), (5), (3), (-1), (-1), (4), (5), (2)
select val < 2.4 and val > 2.3393 from (select entropy(vals) as val from test.defaults);


CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
    vals DateTime
) ENGINE = Memory;
insert into test.defaults values (toDateTime('2016-06-15 23:00:00')), (toDateTime('2016-06-15 23:00:00')), (toDateTime('2016-06-15 23:00:00')), (toDateTime('2016-06-15 23:00:00')), (toDateTime('2016-06-15 24:00:00')), (toDateTime('2016-06-15 24:00:00')), (toDateTime('2016-06-15 24:00:00')), (toDateTime('2017-06-15 24:00:00')), (toDateTime('2017-06-15 24:00:00')), (toDateTime('2018-06-15 24:00:00')), (toDateTime('2018-06-15 24:00:00')), (toDateTime('2019-06-15 24:00:00'));
select val < 2.189 and val > 2.1886 from (select entropy(vals) as val from test.defaults);

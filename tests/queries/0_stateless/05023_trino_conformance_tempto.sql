-- Conformance queries derived from the convention-based (Tempto) product tests
-- of Presto and Trino (https://github.com/prestodb/presto and
-- https://github.com/trinodb/trino, both Apache License 2.0), executed against
-- the fixture tables of those tests (TPC-H nation/region are scale-independent).
-- Expected results verified against the .result files of the original tests.

SET allow_experimental_trino_dialect = 1;
SET dialect = 'trino';

CREATE TABLE nation (n_nationkey Int64, n_name String, n_regionkey Int64, n_comment String) ENGINE = Memory;
INSERT INTO nation VALUES
    (0, 'ALGERIA', 0, ' haggle. carefully final deposits detect slyly agai'), (1, 'ARGENTINA', 1, 'al foxes promise slyly according to the regular accounts. bold requests alon'),
    (2, 'BRAZIL', 1, 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special '), (3, 'CANADA', 1, 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold'),
    (4, 'EGYPT', 4, 'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d'), (5, 'ETHIOPIA', 0, 'ven packages wake quickly. regu'),
    (6, 'FRANCE', 3, 'refully final requests. regular, ironi'), (7, 'GERMANY', 3, 'l platelets. regular accounts x-ray: unusual, regular acco'),
    (8, 'INDIA', 2, 'ss excuses cajole slyly across the packages. deposits print aroun'), (9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull'),
    (10, 'IRAN', 4, 'efully alongside of the slyly final dependencies. '), (11, 'IRAQ', 4, 'nic deposits boost atop the quickly final requests? quickly regula'),
    (12, 'JAPAN', 2, 'ously. final, express gifts cajole a'), (13, 'JORDAN', 4, 'ic deposits are blithely about the carefully regular pa'),
    (14, 'KENYA', 0, ' pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t'), (15, 'MOROCCO', 0, 'rns. blithely bold courts among the closely regular packages use furiously bold platelets?'),
    (16, 'MOZAMBIQUE', 0, 's. ironic, unusual asymptotes wake blithely r'), (17, 'PERU', 1, 'platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun'),
    (18, 'CHINA', 2, 'c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos'), (19, 'ROMANIA', 3, 'ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account'),
    (20, 'SAUDI ARABIA', 4, 'ts. silent requests haggle. closely express packages sleep across the blithely'), (21, 'VIETNAM', 2, 'hely enticingly express accounts. even, final '),
    (22, 'RUSSIA', 3, ' requests against the platelets use never according to the quickly regular pint'), (23, 'UNITED KINGDOM', 3, 'eans boost carefully special requests. accounts are. carefull'),
    (24, 'UNITED STATES', 1, 'y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be');
CREATE TABLE region (r_regionkey Int64, r_name String, r_comment String) ENGINE = Memory;
INSERT INTO region VALUES
    (0, 'AFRICA', 'lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to '),
    (1, 'AMERICA', 'hs use ironic, even requests. s'),
    (2, 'ASIA', 'ges. thinly even pinto beans ca'),
    (3, 'EUROPE', 'ly final courts cajole furiously final excuse'),
    (4, 'MIDDLE EAST', 'uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl');
CREATE TABLE datatype (c_bigint Nullable(Int64), c_double Nullable(Float64), c_string Nullable(String), c_date Nullable(Date32), c_timestamp Nullable(DateTime64(3)), c_boolean Nullable(Bool), c_short_decimal Nullable(Decimal(5,2)), c_long_decimal Nullable(Decimal(30,10))) ENGINE = Memory;
INSERT INTO datatype VALUES
    (12, 12.25, 'String1', '1999-01-08', '1999-01-08 02:05:06', true, 123.22, 12345678901234567890.0123456789),
    (25, 55.52, 'test', '1952-01-05', '1989-01-08 04:05:06', false, 321.21, -12345678901234567890.0123456789),
    (964, 0.245, 'Again', '1936-02-08', '2005-01-09 04:05:06', false, 333.82, 98765432109876543210.9876543210),
    (100, 12.25, 'testing', '1949-07-08', '2002-01-07 01:05:06', true, -393.22, -98765432109876543210.9876543210),
    (100, 99.8777, 'AGAIN', '1987-04-09', '2010-01-02 04:03:06', true, 000.00, 00000000000000000000.0000000000),
    (5252, 12.25, 'sample', '1987-04-09', '2010-01-02 04:03:06', true, 123.00, 00000000000000000001.0000000000),
    (100, 9.8777, 'STRING1', '1923-04-08', '2010-01-02 05:09:06', true, 010.01, 00000000000000000002.0000000000),
    (8996, 98.8777, 'again', '1987-04-09', '2010-01-02 04:03:06', false, -000.01, 99999999999999999999.9999999999),
    (100, 12.8788, 'string1', '1922-04-02', '2010-01-02 02:05:06', true, 999.99, -99999999999999999999.9999999999),
    (5748, 67.87, 'sample', '1987-04-06', '2010-01-02 04:03:06', true, -999.99, 00000000000000000000.0000000001),
    (5748, 67.87, 'Sample', '1987-04-06', '2010-01-02 04:03:06', true, 181.18, -00000000000000000000.0000000001),
    (5748, 67.87, 'sample', '1987-04-06', '2010-01-02 04:03:06', true, 181.18, 12345678901234567890.0123456789),
    (5748, 67.87, 'sample', '1987-04-06', '2010-01-02 04:03:06', true, 181.18, 12345678901234567890.0123456789),
    (5000, 67.87, 'testing', NULL, '2010-01-02 04:03:06', NULL, NULL, NULL),
    (6000, NULL, NULL, '1987-04-06', NULL, true, NULL, NULL),
    (NULL, 98.52, NULL, NULL, NULL, true, 181.18, NULL);
CREATE TABLE workers (id_employee Nullable(Int32), first_name Nullable(String), last_name Nullable(String), date_of_employment Nullable(String), department Nullable(Int32), id_department Nullable(Int32), name Nullable(String), salary Nullable(Int32)) ENGINE = Memory;
INSERT INTO workers VALUES
    (NULL, NULL, NULL, NULL, NULL, 1, 'Marketing', 4000), (2, 'Ann', 'Turner', '2000-05-28', 2, 2, 'R&D', 5000),
    (3, 'Martin', 'Smith', '2000-05-28', 2, 2, 'R&D', 5000), (NULL, NULL, NULL, NULL, NULL, 3, 'Finance', 3000),
    (4, 'Joana', 'Donne', '2002-04-05', 4, 4, 'IT', 4000), (5, 'Kate', 'Grant', '2001-04-06', 5, 5, 'HR', 2000),
    (6, 'Christopher', 'Johnson', '2001-04-06', 5, 5, 'HR', 2000), (NULL, NULL, NULL, NULL, NULL, 6, 'PR', 3000),
    (7, 'George', 'Cage', '2003-10-09', 7, 7, 'CustomerService', 2300), (8, 'Jacob', 'Brown', '2003-10-09', 8, 8, 'Production', 2400),
    (9, 'John', 'Black', '2004-05-09', 9, 9, 'Quality', 3400), (NULL, NULL, NULL, NULL, NULL, 10, 'Sales', 3500),
    (10, 'Charlie', 'Page', '2000-11-12', 11, NULL, NULL, NULL), (1, 'Mary', 'Parker', '1999-04-03', 12, NULL, NULL, NULL);
CREATE TABLE empty (c1 Nullable(Int64), c2 Nullable(String)) ENGINE = Memory;

SELECT '-- aggregate';
select max(upper(c_string)), min(upper(c_string)) from datatype;
select avg(c_bigint), avg(c_double) from datatype;

SELECT '-- array_functions';
SELECT timezone_hour(TIMESTAMP '2001-08-22 03:04:05.321' at time zone 'Asia/Oral'), timezone_minute(TIMESTAMP '2001-08-22 03:04:05.321' at time zone 'Asia/Oral');

SELECT '-- convertion_functions';
SELECT CAST(10 as VARCHAR);
SELECT TRY_CAST(10 as VARCHAR), TRY_CAST('ala' as BIGINT);

SELECT '-- distinct';
SELECT * FROM (SELECT DISTINCT n_regionkey FROM nation) ORDER BY ALL;

SELECT '-- empty_table';
SELECT COUNT(DISTINCT c2) AS cnt FROM empty;
SELECT COUNT(DISTINCT c1) AS cnt FROM empty;
SELECT min(c1), max(c1) from empty;
SELECT c1*c1 from empty;
SELECT sum(c1), c2 from empty group by c2;
SELECT count(*), c1 from empty group by c1;
SELECT sum(c1) from empty;
SELECT sqrt(c1) from empty;
SELECT count(*) from empty;
SELECT * from empty;
SELECT SUM(cnt) FROM (SELECT COUNT(*) AS cnt FROM empty) foo;

SELECT '-- functions';
select case when true then 33 end;

SELECT '-- group-by';
SELECT * FROM (SELECT n_regionkey, COUNT(null) FROM nation WHERE n_nationkey > 5 GROUP BY n_regionkey) ORDER BY ALL;
SELECT COUNT(n_regionkey) FROM nation WHERE 1=2 HAVING SUM(n_regionkey) IS NULL;
SELECT * FROM (SELECT COUNT(*), n_regionkey, n_nationkey FROM nation WHERE n_regionkey < 2 GROUP BY n_nationkey, n_regionkey ORDER BY n_regionkey, n_nationkey DESC) ORDER BY ALL;

SELECT '-- horology_functions';
SELECT timezone_hour(TIMESTAMP '2001-08-22 03:04:05.321' at time zone 'Asia/Oral'), timezone_minute(TIMESTAMP '2001-08-22 03:04:05.321' at time zone 'Asia/Oral');
SELECT extract(day from TIMESTAMP '2001-08-22 03:04:05.321');

SELECT '-- limit';
SELECT n_nationkey from nation ORDER BY n_nationkey DESC LIMIT 5;

SELECT '-- map_functions';
select MAP(ARRAY ['ala', 'kot'], ARRAY[3, 4]) ['kot'];

SELECT '-- math_functions';
select 2+2, 5-2, 3*3, 8/2, 8%3;

SELECT '-- set_operation';
SELECT * FROM (SELECT n_name FROM nation WHERE n_nationkey = 17 EXCEPT SELECT n_name FROM nation WHERE n_regionkey = 2 UNION (SELECT n_name FROM nation WHERE n_regionkey = 2 INTERSECT SELECT n_name FROM nation WHERE n_nationkey > 15)) ORDER BY ALL;
SELECT * FROM (SELECT n_name FROM nation WHERE n_nationkey = 17 EXCEPT SELECT n_name FROM nation WHERE n_regionkey = 2 UNION ALL (SELECT n_name FROM nation WHERE n_regionkey = 2 INTERSECT SELECT n_name FROM nation WHERE n_nationkey > 15)) ORDER BY ALL;
SELECT * FROM (SELECT id_employee FROM workers EXCEPT SELECT department FROM workers where department IS NOT NULL) ORDER BY ALL;
SELECT * FROM (SELECT n_name FROM nation WHERE n_nationkey = 17 INTERSECT SELECT n_name FROM nation WHERE n_regionkey = 1 UNION SELECT n_name FROM nation WHERE n_regionkey = 2) ORDER BY ALL;
SELECT * FROM (SELECT id_employee FROM workers INTERSECT SELECT department FROM workers) ORDER BY ALL;

SELECT '-- union';
SELECT * FROM (SELECT * FROM nation UNION ALL SELECT * FROM nation) ORDER BY ALL;
SELECT * FROM (SELECT * FROM nation UNION DISTINCT SELECT * FROM nation) ORDER BY ALL;
SELECT * FROM (SELECT count(*) FROM nation UNION ALL SELECT sum(n_nationkey) FROM nation GROUP BY n_regionkey UNION ALL SELECT n_regionkey FROM nation) ORDER BY ALL;
SELECT count(*) FROM nation UNION ALL SELECT sum(n_nationkey) FROM nation GROUP BY n_regionkey UNION ALL SELECT n_regionkey FROM nation ORDER BY 1 DESC;
SELECT * FROM (SELECT count(*) FROM nation UNION ALL SELECT sum(n_nationkey) FROM nation GROUP BY n_regionkey) ORDER BY ALL;

SELECT '-- with_clause';
WITH nested AS (SELECT * FROM nation) SELECT count(*) FROM (select * FROM nested) as a;

SELECT '-- presto';
select avg(distinct c_bigint), avg(distinct c_double) from datatype;
select count(c_bigint),count(c_double),count(c_string),count(c_date),count(c_timestamp),count(c_boolean) from datatype;
select count(distinct c_bigint),count(distinct c_double),count(distinct c_string),count(distinct c_date),count(distinct c_timestamp),count(distinct c_boolean) from datatype;
select count(c_string), max(c_double), avg(c_bigint) from datatype;
select skewness(c_bigint), skewness(c_double) from datatype;
select stddev_pop(c_bigint), stddev_pop(c_double) from datatype;
select stddev_pop(distinct c_bigint), stddev_pop(distinct c_double) from datatype;
select stddev_samp(c_bigint), stddev_samp(c_double) from datatype;
select stddev_samp(distinct c_bigint), stddev_samp(distinct c_double) from datatype;
select sum(c_bigint), sum(c_double) from datatype;
select sum(distinct c_bigint), sum(distinct c_double) from datatype;
select var_pop(c_bigint), var_pop(c_double) from datatype;
select var_pop(distinct c_bigint), var_pop(distinct c_double) from datatype;
select var_samp(c_bigint), var_samp(c_double) from datatype;
select var_samp(distinct c_bigint), var_samp(distinct c_double) from datatype;
select variance(c_bigint), variance(c_double) from datatype;
select variance(distinct c_bigint), variance(distinct c_double) from datatype;
SELECT COUNT(DISTINCT n_regionkey), COUNT(DISTINCT n_name), MIN(DISTINCT n_nationkey) FROM nation;
SELECT COUNT(DISTINCT n_regionkey), COUNT(DISTINCT n_regionkey) FROM nation;
SELECT COUNT(DISTINCT n_regionkey), COUNT(*) FROM nation;
SELECT * FROM (SELECT DISTINCT n_regionkey, COUNT(*) FROM nation WHERE n_nationkey > 0 GROUP BY n_regionkey) ORDER BY ALL;
SELECT * FROM (SELECT n_regionkey, COUNT(DISTINCT n_name) FROM nation GROUP BY n_regionkey HAVING n_regionkey < 4) ORDER BY ALL;
SELECT * FROM (SELECT DISTINCT r_name FROM region) ORDER BY ALL;
SELECT DISTINCT n_regionkey FROM nation ORDER BY n_regionkey;
select count(*), count(n_regionkey), min(n_regionkey), max(n_regionkey), sum(n_regionkey) from nation;
SELECT * FROM (select n_regionkey, count(*) from nation group by 1 having sum(n_regionkey) > 5 and sum(n_regionkey) < 20) ORDER BY ALL;
SELECT * FROM (select n_regionkey, count(*), sum(n_nationkey) from nation group by 1) ORDER BY ALL;
select count(*), sum(n_nationkey) from nation where 1=2 group by n_regionkey;
SELECT * FROM (select n_regionkey, count(*), sum(n_regionkey) from nation where n_regionkey > 2 group by 1) ORDER BY ALL;
select count(*), sum(n_nationkey) from nation where 1=2;
select 2 from nation group by 1;
SELECT * FROM (SELECT n_regionkey FROM (SELECT n_regionkey, COUNT(*) cnt FROM nation GROUP BY n_regionkey) t GROUP BY n_regionkey HAVING n_regionkey < 3 AND COUNT(cnt) > 0) ORDER BY ALL;
SELECT COUNT(*) FROM workers HAVING SUM(salary * 2)/COUNT(*) > 0;
SELECT * FROM (SELECT COUNT(*) FROM workers GROUP BY id_department * 2 HAVING SUM(log10(salary + 1)) > 0) ORDER BY ALL;
SELECT * FROM (SELECT COUNT(*) FROM workers GROUP BY salary * id_department HAVING salary * id_department IS NOT NULL) ORDER BY ALL;
SELECT first_name, COUNT(*) FROM workers GROUP BY first_name HAVING first_name IS NULL;
SELECT * FROM (SELECT id_department, COUNT(*) FROM workers GROUP BY id_department HAVING COUNT(*) > 1 ORDER BY id_department desc) ORDER BY ALL;
SELECT COUNT(*) FROM nation HAVING COUNT(*) > 20;
SELECT * FROM (select n_name, r_name from nation cross join region) ORDER BY ALL;
SELECT * FROM (select n_name, r_name from nation join region on nation.n_regionkey = region.r_regionkey) ORDER BY ALL;
select count(*) from nation join region on nation.n_regionkey = region.r_regionkey;
SELECT * FROM (select * from nation join region on nation.n_regionkey = region.r_regionkey) ORDER BY ALL;
SELECT * FROM (select n_name, r_name from nation join region on nation.n_regionkey = region.r_regionkey where n_name > 'E') ORDER BY ALL;
SELECT * FROM (select n.n_name, r.r_name from nation n, region r where n.n_regionkey = r.r_regionkey) ORDER BY ALL;
SELECT * FROM (SELECT n_name, r_name FROM nation, region WHERE r_regionkey > n_nationkey) ORDER BY ALL;
SELECT * FROM (SELECT n_name, r_name FROM nation, region WHERE r_regionkey != n_nationkey) ORDER BY ALL;
SELECT * FROM (select n_name, department, name, salary from nation, workers where n_nationkey = department) ORDER BY ALL;
SELECT n_name, r_name FROM nation LEFT JOIN region ON n_nationkey = r_regionkey WHERE r_name > 'G';
SELECT * FROM (SELECT n_name FROM nation LEFT JOIN region ON n_nationkey = r_regionkey WHERE r_name is not null) ORDER BY ALL;
SELECT * FROM (SELECT n_name FROM nation LEFT JOIN region ON n_nationkey = r_regionkey WHERE r_name is null) ORDER BY ALL;
SELECT * FROM (select n_name, r_name from nation left outer join region on n_nationkey = r_regionkey) ORDER BY ALL;
SELECT * FROM (select n_name, department, name, salary from nation right outer join workers on n_nationkey = department) ORDER BY ALL;
SELECT * FROM (select n_name, r_name from region right outer join nation on n_nationkey = r_regionkey) ORDER BY ALL;
SELECT * FROM (select n_name from nation where n_nationkey in (select r_regionkey from region)) ORDER BY ALL;
SELECT n_nationkey FROM nation WHERE n_name < 'INDIA' ORDER BY n_nationkey LIMIT 3;
SELECT COUNT(*) FROM (SELECT * FROM nation LIMIT 2) AS foo LIMIT 5;
SELECT COUNT(*), n_regionkey FROM nation GROUP BY n_regionkey ORDER BY n_regionkey DESC LIMIT 2;
SELECT foo.c, foo.n_regionkey FROM (SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY n_regionkey LIMIT 2) foo;
SELECT COUNT(*) FROM (SELECT * FROM nation n1 JOIN nation n2 ON n1.n_regionkey = n2.n_regionkey LIMIT 5) foo;
SELECT COUNT(*) FROM (SELECT * FROM nation LIMIT 0) foo;
SELECT COUNT(*) FROM (SELECT * FROM nation LIMIT 10) t1;
select * from (select cast(null as bigint) union all select 1) T order by 1 asc;
select * from (select cast(null as bigint) union all select 1) T order by 1 asc nulls first;
select * from (select cast(null as bigint) union all select 1) T order by 1 asc nulls last;
select * from (select cast(null as bigint) union all select 1) T order by 1;
select * from (select cast(null as bigint) union all select 1) T order by 1 nulls first;
select * from (select cast(null as bigint) union all select 1) T order by 1 nulls last;
select * from (select cast(null as bigint) union all select 1) T order by 1 desc nulls first;
select * from (select cast(null as bigint) union all select 1) T order by 1 desc nulls last;
SELECT COUNT(10), MAX(50), MIN(90.0);
SELECT 1, 1.1, 100*5.1, 'a', 'dummy values', TRUE, FALSE;
SELECT abs(-10.0E0), log2(4), TRUE AND FALSE, TRUE OR FALSE;
SELECT MIN(10), 3 as col1 GROUP BY 2 HAVING 6 > 5 ORDER BY 1;
SELECT * FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 4*5 UNION ALL SELECT -5) ORDER BY ALL;
SELECT 1 WHERE TRUE AND 2=2;
SELECT COUNT(*), 1 WHERE FALSE;
SELECT * FROM (WITH wnation AS (SELECT n_nationkey, n_regionkey FROM nation), wregion AS (SELECT r_regionkey, r_name FROM region) select n.n_nationkey, r.r_regionkey from wnation n join wregion r on n.n_regionkey = r.r_regionkey where r.r_name = 'AFRICA') ORDER BY ALL;
WITH w1 AS (select * from nation), w2 AS (select * from w1) select count(*) from w1, w2;
WITH wregion AS (select min(n_regionkey) from nation where n_name >= 'N') select r_regionkey, r_name from region where r_regionkey IN (SELECT * FROM wregion);
SELECT * FROM (WITH wnation AS (SELECT n_name, n_nationkey, n_regionkey FROM nation) SELECT n1.n_name, n2.n_name FROM wnation n1 JOIN wnation n2 ON n1.n_nationkey=n2.n_regionkey) ORDER BY ALL;
WITH ordered AS (select n_nationkey a, n_regionkey b, n_name c from nation order by 1,2 limit 10) select * from ordered order by 1,2 limit 5;
WITH ct AS (SELECT * FROM region) SELECT n_name FROM nation where n_nationkey = 0;

DROP TABLE nation;
DROP TABLE region;
DROP TABLE datatype;
DROP TABLE workers;
DROP TABLE empty;

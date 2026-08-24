DROP TABLE IF EXISTS insert;
CREATE TABLE insert (i UInt64, s String, u UUID, d Date, t DateTime, a Array(UInt32)) ENGINE = Memory;

INSERT INTO insert VALUES (1, 'Hello', 'ab41bdd6-5cd4-11e7-907b-a6006ad3dba0', '2016-01-01', '2016-01-02 03:04:05', [1, 2, 3]), (1 + 1, concat('Hello', ', world'), toUUID('00000000-0000-0000-0000-000000000000'), toDate('2016-01-01') + 1, toStartOfMinute(toDateTime('2016-01-02 03:04:05')), [[0,1],[2]][1]), (round(pi()), concat('hello', ', world!'), toUUID(toString('ab41bdd6-5cd4-11e7-907b-a6006ad3dba0')), toDate(toDateTime('2016-01-03 03:04:05')), toStartOfHour(toDateTime('2016-01-02 03:04:05')), []), (4, 'World', 'ab41bdd6-5cd4-11e7-907b-a6006ad3dba0', '2016-01-04', '2016-12-11 10:09:08', [3,2,1]);

SELECT * FROM insert ORDER BY i;
DROP TABLE insert;

-- Test the case where the VALUES are delimited by semicolon and a query follows
-- w/o newline. With most formats the query in the same line would be ignored or
-- lead to an error, but VALUES are an exception and support semicolon delimiter,
-- in addition to the newline.
create table if not exists t_306 (a int) engine Memory;
insert into t_306 values (1); select 11111;
select * from t_306;
drop table if exists t_306;

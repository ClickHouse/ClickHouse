drop table if exists proj;

CREATE TABLE proj(date Date, PROJECTION maxdate( SELECT max(date) GROUP BY date )) ENGINE = MergeTree ORDER BY tuple() as select toDate('2012-10-24')-number%100 from numbers(1e2);

SELECT max(date) FROM proj PREWHERE date != '2012-10-24';

drop table proj;

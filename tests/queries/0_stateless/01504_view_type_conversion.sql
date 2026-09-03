DROP TABLE IF EXISTS testv;

create view testv(a UInt32) as select number a from numbers(10);
select groupArray(a) from testv;

DROP TABLE testv;

create view testv(a String) as select number a from numbers(10);
select groupArray(a) from testv;

DROP TABLE testv;

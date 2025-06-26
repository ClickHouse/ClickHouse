DROP TABLE IF EXISTS A;

create table A (A Int64, B Int64, S String) Engine=MergeTree order by A 
as select number,number, toString(arrayMap(i->cityHash64(i*number), range(10))) from numbers(1e6);

SET join_algorithm = 'grace_hash', grace_hash_join_initial_buckets=128, grace_hash_join_max_buckets=256;

select * from A a join A as b on a.A = b.A limit 1 FORMAT Null;

SET join_algorithm = 'grace_hash', grace_hash_join_initial_buckets=128, grace_hash_join_max_buckets=128;

select * from A a join A as b on a.A = b.A limit 1 FORMAT Null;

DROP TABLE A;

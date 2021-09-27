drop table if exists test_23634;

set force_primary_key=1;

CREATE TABLE test_23634 (id Nullable(String), s Nullable(String), s1 Nullable(String))
ENGINE = MergeTree() ORDER BY (id,s) SETTINGS allow_nullable_key = 1;

INSERT into test_23634 values ('s','s','s'), (null,'s1','s1'), (null,null,'s2'), (null,null,null);

select * from test_23634 where id !='';

select * from test_23634 where id !='' and s != '';

select * from test_23634 where id !='' and s != '' and s1 != '';

drop table test_23634;

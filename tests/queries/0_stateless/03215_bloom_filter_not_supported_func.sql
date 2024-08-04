drop table if exists t;

create table t ( 
  c Int32,
  index x1 (c) type bloom_filter
) engine=MergeTree order by c as select 1;

SELECT count() FROM t WHERE cast(c=1 or c=9999 as Bool) 
settings use_skip_indexes=0;

SELECT count() FROM t WHERE cast(c=1 or c=9999 as Bool) 
settings use_skip_indexes=1;

drop table t;
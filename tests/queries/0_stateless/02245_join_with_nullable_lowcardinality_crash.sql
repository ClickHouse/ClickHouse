drop table if exists with_nullable;
drop table if exists without_nullable;

CREATE TABLE with_nullable
( timestamp UInt32,
  country LowCardinality(Nullable(String)) ) ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE  without_nullable
( timestamp UInt32,
  country LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();

insert into with_nullable values(0,'f'),(0,'usa');
insert into without_nullable values(0,'usa'),(0,'us2a');

select if(t0.country is null ,t2.country,t0.country) "country"
from without_nullable t0 right outer join with_nullable t2 on t0.country=t2.country
ORDER BY 1 DESC;

drop table with_nullable;
drop table without_nullable;


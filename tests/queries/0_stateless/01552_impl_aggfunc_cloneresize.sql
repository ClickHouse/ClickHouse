drop table if EXISTS test_bm;

drop table if EXISTS test_bm_join;

create table test_bm(
	dim UInt64,
	id UInt64 ) 
ENGINE = MergeTree()
ORDER BY( dim, id )
SETTINGS index_granularity = 8192;

create table test_bm_join( 
  dim UInt64,
  id UInt64 )
ENGINE = MergeTree()
ORDER BY(dim,id)
SETTINGS index_granularity = 8192;

insert into test_bm VALUES (1,1),(2,2),(3,3),(4,4);

select
	dim ,
	sum(idnum)
from
	test_bm_join
right join(
	select
		dim,
		bitmapOrCardinality(ids,ids2) as idnum
	from
		(
		select
			dim,
			groupBitmapState(toUInt64(id)) as ids
		FROM
			test_bm
		where
			dim >2
		group by
			dim ) A all
	right join (
		select
			dim,
			groupBitmapState(toUInt64(id)) as ids2
		FROM
			test_bm
		where
			dim < 2
		group by
			dim ) B
	using(dim) ) C
using(dim)
group by dim;

drop table test_bm;

drop table test_bm_join;

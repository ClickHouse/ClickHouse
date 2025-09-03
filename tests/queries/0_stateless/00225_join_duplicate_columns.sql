-- The following queries use very weird block structure:
-- __table3.b UInt8 UInt8(size = 1), __table3.b UInt8 Const(size = 1, UInt8(size = 1)), __table3.c UInt8 Const(size = 1, UInt8(size = 1))
-- That leads to a pretty legit error in ConcurrentHashJoin within a call to Block::cloneEmpty():
-- Code: 352. DB::Exception: Block structure mismatch in (columns with identical name must have identical structure) stream: different columns:
-- __table3.b UInt8 UInt8(size = 0)
-- __table3.b UInt8 Const(size = 0, UInt8(size = 1))
-- So let's disable parallel_hash.
SET join_algorithm = 'hash,grace_hash,partial_merge,full_sorting_merge';

select b from (select 1 as a, 42 as c) js1 any left join (select 2 as b, 2 as b, 41 as c) js2 using c;
select b from (select 1 as a, 42 as c) js1 any left join (select 2 as b, 2 as b, 42 as c) js2 using c;

select c,a,a,b,b from
  (select 1 as a, 1 as a, 42 as c group by c order by a,c) js1
 any left join
  (select 2 as b, 2 as b, 41 as c group by c order by b,c) js2
 using c
 order by b;

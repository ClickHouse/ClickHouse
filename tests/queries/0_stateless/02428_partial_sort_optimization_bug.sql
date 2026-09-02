create table partial_sort_opt_bug (x UInt64) engine = MergeTree order by tuple() settings index_granularity = 1000;

insert into partial_sort_opt_bug select number + 100000 from numbers(4000);

insert into partial_sort_opt_bug select number from numbers(1000);
insert into partial_sort_opt_bug select number + 200000 from numbers(3000);
insert into partial_sort_opt_bug select number + 1000 from numbers(4000);
optimize table partial_sort_opt_bug final;

select x from partial_sort_opt_bug order by x limit 2000 settings max_block_size = 4000;


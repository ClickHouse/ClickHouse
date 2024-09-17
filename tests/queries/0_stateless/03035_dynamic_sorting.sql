set allow_experimental_dynamic_type = 1;

drop table if exists test;
create table test (d1 Dynamic(max_types=2), d2 Dynamic(max_types=2)) engine=Memory;

insert into test values (42, 42), (42, 43), (43, 42), ('abc', 'abc'), ('abc', 'abd'), ('abd', 'abc'),
([1,2,3], [1,2,3]), ([1,2,3], [1,2,4]), ([1,2,4], [1,2,3]),
('2020-01-01', '2020-01-01'), ('2020-01-01', '2020-01-02'), ('2020-01-02', '2020-01-01'),
(NULL, NULL), (42, 'abc'), ('abc', 42), (42, [1,2,3]), ([1,2,3], 42), (42, NULL), (NULL, 42),
('abc', [1,2,3]), ([1,2,3], 'abc'), ('abc', NULL), (NULL, 'abc'), ([1,2,3], NULL), (NULL, [1,2,3]),
(42, '2020-01-01'), ('2020-01-01', 42), ('2020-01-01', 'abc'), ('abc', '2020-01-01'),
('2020-01-01', [1,2,3]), ([1,2,3], '2020-01-01'), ('2020-01-01', NULL), (NULL, '2020-01-01');

select 'order by d1 nulls first';
select d1, dynamicType(d1), isDynamicElementInSharedData(d1) from test order by d1 nulls first;

select 'order by d1 nulls last';
select d1, dynamicType(d1), isDynamicElementInSharedData(d1) from test order by d1 nulls last;

select 'order by d2 nulls first';
select d2, dynamicType(d2), isDynamicElementInSharedData(d2) from test order by d2 nulls first;

select 'order by d2 nulls last';
select d2, dynamicType(d2), isDynamicElementInSharedData(d2) from test order by d2 nulls last;


select 'order by d1, d2 nulls first';
select d1, d2, dynamicType(d1), isDynamicElementInSharedData(d1), dynamicType(d2), isDynamicElementInSharedData(d2) from test order by d1, d2 nulls first;

select 'order by d1, d2 nulls last';
select d1, d2, dynamicType(d1), isDynamicElementInSharedData(d1), dynamicType(d2), isDynamicElementInSharedData(d2) from test order by d1, d2 nulls last;

select 'order by d2, d1 nulls first';
select d1, d2, dynamicType(d1), isDynamicElementInSharedData(d1), dynamicType(d2), isDynamicElementInSharedData(d2) from test order by d2, d1 nulls first;

select 'order by d2, d1 nulls last';
select d1, d2, dynamicType(d1), isDynamicElementInSharedData(d1), dynamicType(d2), isDynamicElementInSharedData(d2) from test order by d2, d1 nulls last;

drop table test;


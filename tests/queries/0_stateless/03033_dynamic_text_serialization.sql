set allow_experimental_dynamic_type = 1;

select 'JSON';
select d, dynamicType(d) from format(JSONEachRow, 'd Dynamic', $$
{"d" : 42}
{"d" : 42.42}
{"d" : "str"}
{"d" : [1, 2, 3]}
{"d" : "2020-01-01"}
{"d" : "2020-01-01 10:00:00"}
{"d" : {"a" : 42, "b" : "str"}}
{"d" : {"a" : 43}}
{"d" : {"a" : 44, "c" : [1, 2, 3]}}
{"d" : [1, "str", [1, 2, 3]]}
{"d" : null}
{"d" : true}
$$) format JSONEachRow;

select d, dynamicType(d), isDynamicElementInSharedData(d) from format(JSONEachRow, 'd Dynamic(max_types=2)', $$
{"d" : 42}
{"d" : 42.42}
{"d" : "str"}
{"d" : null}
{"d" : true}
$$) format JSONEachRow;

select 'CSV';
select d, dynamicType(d) from format(CSV, 'd Dynamic',
$$42
42.42
"str"
"[1, 2, 3]"
"2020-01-01"
"2020-01-01 10:00:00"
"[1, 'str', [1, 2, 3]]"
\N
true
$$) format CSV;

select 'TSV';
select d, dynamicType(d) from format(TSV, 'd Dynamic',
$$42
42.42
str
[1, 2, 3]
2020-01-01
2020-01-01 10:00:00
[1, 'str', [1, 2, 3]]
\N
true
$$) format TSV;

select 'Values';
select d, dynamicType(d) from format(Values, 'd Dynamic', $$
(42)
(42.42)
('str')
([1, 2, 3])
('2020-01-01')
('2020-01-01 10:00:00')
(NULL)
(true)
$$) format Values;
select '';

select 'Cast using parsing';
drop table if exists test;
create table test (s String) engine=Memory;
insert into test values ('42'), ('42.42'), ('[1, 2, 3]'), ('2020-01-01'), ('2020-01-01 10:00:00'), ('NULL'), ('true');
set cast_string_to_dynamic_use_inference=1;
select s::Dynamic as d, dynamicType(d) from test;
select s::Dynamic(max_types=3) as d, dynamicType(d), isDynamicElementInSharedData(d) from test;
drop table test;


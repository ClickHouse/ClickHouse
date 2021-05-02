create type MyType1 as int;

create type MyType2 as String;

create table TestTable (first MyType1, second MyType2) ENGINE = MergeTree() ORDER BY first;

insert into TestTable values(10, 'test');

select * from TestTable;

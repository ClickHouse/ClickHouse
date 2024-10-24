drop table if exists x;
create table x
(
    kv Map(LowCardinality(String), LowCardinality(String)),
    k Array(LowCardinality(String)) alias mapKeys(kv),
    v Array(LowCardinality(String)) alias mapValues(kv)
) engine=Memory();
insert into x values (map('foo', 'bar'));
select k, v from x settings optimize_functions_to_subcolumns=1;

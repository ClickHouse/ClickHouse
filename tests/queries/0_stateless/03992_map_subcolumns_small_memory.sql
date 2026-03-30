-- Test: Map subcolumns reading with small dataset and Memory engine

drop table if exists t;
create table t (id UInt64, m Map(String, UInt64)) engine = Memory;
insert into t values
    (1, {'a' : 1, 'b' : 2, 'c' : 3}),
    (2, {'a' : 10, 'b' : 20}),
    (3, {}),
    (4, {'c' : 100, 'd' : 200}),
    (5, {'a' : 5}),
    (6, {'a' : 1, 'b' : 2, 'c' : 3, 'd' : 4, 'e' : 5}),
    (7, {'b' : 42}),
    (8, {'a' : 0, 'd' : 0}),
    (9, {}),
    (10, {'a' : 99, 'b' : 98, 'c' : 97, 'd' : 96, 'e' : 95});

select 'memory: m';
select m from t order by id;
select 'memory: m.keys';
select m.keys from t order by id;
select 'memory: m.values';
select m.values from t order by id;
select 'memory: m.size0';
select m.size0 from t order by id;
select 'memory: m.key_a';
select m.key_a from t order by id;
select 'memory: m.key_d';
select m.key_d from t order by id;
select 'memory: m.key_nonexistent';
select m.key_nonexistent from t order by id;

select 'memory: m.keys, m.values';
select m.keys, m.values from t order by id;
select 'memory: m.size0, m.key_a';
select m.size0, m.key_a from t order by id;
select 'memory: m, m.keys';
select m, m.keys from t order by id;
select 'memory: m, m.key_a';
select m, m.key_a from t order by id;
select 'memory: m.keys, m';
select m.keys, m from t order by id;
select 'memory: m.key_a, m.size0, m';
select m.key_a, m.size0, m from t order by id;
select 'memory: m.key_a, m.key_b, m.key_c';
select m.key_a, m.key_b, m.key_c from t order by id;
select 'memory: m.key_a, m.key_d';
select m.key_a, m.key_d from t order by id;
select 'memory: m, m.keys, m.values, m.size0, m.key_a';
select m, m.keys, m.values, m.size0, m.key_a from t order by id;

drop table t;

-- Map inside Tuple
drop table if exists t;
create table t (id UInt64, data Tuple(m Map(String, UInt64))) engine = Memory;
insert into t values
    (1, ({'a' : 1, 'b' : 2, 'c' : 3})),
    (2, ({'a' : 10, 'b' : 20})),
    (3, ({})),
    (4, ({'c' : 100, 'd' : 200})),
    (5, ({'a' : 5})),
    (6, ({'a' : 1, 'b' : 2, 'c' : 3, 'd' : 4, 'e' : 5})),
    (7, ({'b' : 42})),
    (8, ({'a' : 0, 'd' : 0})),
    (9, ({})),
    (10, ({'a' : 99, 'b' : 98, 'c' : 97, 'd' : 96, 'e' : 95}));

select 'memory tuple: data.m';
select data.m from t order by id;
select 'memory tuple: data.m.keys';
select data.m.keys from t order by id;
select 'memory tuple: data.m.values';
select data.m.values from t order by id;
select 'memory tuple: data.m.size0';
select data.m.size0 from t order by id;
select 'memory tuple: data.m.key_a';
select data.m.key_a from t order by id;

select 'memory tuple: data.m.key_a, data.m.key_d';
select data.m.key_a, data.m.key_d from t order by id;
select 'memory tuple: data.m, data.m.keys';
select data.m, data.m.keys from t order by id;
select 'memory tuple: data.m.keys, data.m';
select data.m.keys, data.m from t order by id;
select 'memory tuple: data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a';
select data.m, data.m.keys, data.m.values, data.m.size0, data.m.key_a from t order by id;

drop table t;

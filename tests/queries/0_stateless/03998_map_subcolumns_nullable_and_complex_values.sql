-- Test: Map key subcolumns (m.key_X) with Nullable and complex value types.
-- Exercises the Nullable unwrapping and generic extraction paths in DataTypeMapHelpers.
-- Other subcolumns (m.keys, m.values, m.size0) are covered by tests 03992-03996.

-- ==========================================
-- Section 1: Map(String, Nullable(UInt64))
-- Exercises extractValuesVector with Nullable null-map propagation.
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(String, Nullable(UInt64))) engine = Memory;
insert into t values
    (1, {'a' : 1, 'b' : 2, 'c' : 3}),
    (2, {'a' : NULL, 'b' : 20}),
    (3, {}),
    (4, {'c' : NULL, 'd' : NULL}),
    (5, {'a' : 5}),
    (6, {'a' : 1, 'b' : NULL, 'c' : 3, 'd' : NULL, 'e' : 5}),
    (7, {'b' : 42}),
    (8, {'a' : 0, 'd' : 0}),
    (9, {}),
    (10, {'a' : 99, 'b' : NULL, 'c' : 97, 'd' : 96, 'e' : 95});

select 'nullable uint64: m.key_a';
select m.key_a from t order by id;
select 'nullable uint64: m.key_b';
select m.key_b from t order by id;
select 'nullable uint64: m.key_d';
select m.key_d from t order by id;
select 'nullable uint64: m.key_nonexistent';
select m.key_nonexistent from t order by id;
select 'nullable uint64: m.key_a, m.key_b, m.key_c';
select m.key_a, m.key_b, m.key_c from t order by id;

drop table t;

-- ==========================================
-- Section 2: Map(String, Nullable(String))
-- Exercises extractValuesString with Nullable null-map propagation.
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(String, Nullable(String))) engine = Memory;
insert into t values
    (1, {'a' : 'hello', 'b' : 'world'}),
    (2, {'a' : NULL, 'b' : 'test'}),
    (3, {}),
    (4, {'a' : '', 'b' : NULL, 'c' : 'notnull'}),
    (5, {'x' : NULL, 'y' : NULL});

select 'nullable string: m.key_a';
select m.key_a from t order by id;
select 'nullable string: m.key_b';
select m.key_b from t order by id;
select 'nullable string: m.key_x';
select m.key_x from t order by id;
select 'nullable string: m.key_missing';
select m.key_missing from t order by id;

drop table t;

-- ==========================================
-- Section 3: Map(String, Array(UInt64))
-- Exercises extractValuesGeneric fallback for Array values.
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(String, Array(UInt64))) engine = Memory;
insert into t values
    (1, {'a' : [1, 2, 3], 'b' : [10, 20]}),
    (2, {'a' : [], 'c' : [100]}),
    (3, {}),
    (4, {'b' : [7, 8, 9], 'd' : []}),
    (5, {'a' : [42]});

select 'array values: m.key_a';
select m.key_a from t order by id;
select 'array values: m.key_b';
select m.key_b from t order by id;
select 'array values: m.key_missing';
select m.key_missing from t order by id;

drop table t;

-- ==========================================
-- Section 4: Map(String, Tuple(UInt64, String))
-- Exercises extractValuesGeneric fallback for Tuple values.
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(String, Tuple(UInt64, String))) engine = Memory;
insert into t values
    (1, {'a' : (1, 'x'), 'b' : (2, 'y')}),
    (2, {'c' : (10, 'hello')}),
    (3, {}),
    (4, {'a' : (0, ''), 'b' : (99, 'end')});

select 'tuple values: m.key_a';
select m.key_a from t order by id;
select 'tuple values: m.key_b';
select m.key_b from t order by id;
select 'tuple values: m.key_c';
select m.key_c from t order by id;
select 'tuple values: m.key_missing';
select m.key_missing from t order by id;

drop table t;

-- ==========================================
-- Section 5: Map(String, Nullable(Float64))
-- Exercises extractValuesVector<Float64> with Nullable null-map propagation.
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(String, Nullable(Float64))) engine = Memory;
insert into t values
    (1, {'a' : 1.5, 'b' : 2.7}),
    (2, {'a' : NULL, 'b' : inf}),
    (3, {}),
    (4, {'a' : nan, 'c' : -0.0}),
    (5, {'b' : NULL});

select 'nullable float64: m.key_a';
select m.key_a from t order by id;
select 'nullable float64: m.key_b';
select m.key_b from t order by id;
select 'nullable float64: m.key_missing';
select m.key_missing from t order by id;

drop table t;

-- ==========================================
-- Section 6: Map(UInt64, Nullable(String))
-- Numeric key with Nullable string values.
-- Exercises extractValuesString with Nullable + KeyMatcherVector.
-- ==========================================

drop table if exists t;
create table t (id UInt64, m Map(UInt64, Nullable(String))) engine = Memory;
insert into t values
    (1, {1 : 'a', 2 : 'b', 3 : 'c'}),
    (2, {1 : NULL, 2 : 'x'}),
    (3, {}),
    (4, {3 : NULL, 4 : NULL}),
    (5, {1 : '', 2 : 'hello'});

select 'uint64 key nullable string: m.key_1';
select m.key_1 from t order by id;
select 'uint64 key nullable string: m.key_2';
select m.key_2 from t order by id;
select 'uint64 key nullable string: m.key_3';
select m.key_3 from t order by id;
select 'uint64 key nullable string: m.key_999';
select m.key_999 from t order by id;

drop table t;

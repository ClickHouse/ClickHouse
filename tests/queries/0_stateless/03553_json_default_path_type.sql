-- Tests for the `JSON(DEFAULT PATH TYPE T)` type modifier: every value of a non-typed
-- path is coerced/validated against T at insert, and JSON null is normalized to a missing path.

-- The type modifier round-trips through the type name.
select toTypeName(CAST('{}' as JSON(DEFAULT PATH TYPE Int64)));
select toTypeName(CAST('{}' as JSON(max_dynamic_paths=0, DEFAULT PATH TYPE String, d UInt32)));

-- DEFAULT PATH TYPE accepts any type the user can use in a table (including Nullable and Dynamic);
-- only a duplicated DEFAULT PATH TYPE argument is rejected.
create table bad1 (j JSON(DEFAULT PATH TYPE String, DEFAULT PATH TYPE Int64)) engine=Memory; -- {serverError BAD_ARGUMENTS}

-- Non-typed-path values are coerced/validated against the default path type.
-- Missing paths are read as NULL (Nullable(T)).
create table source (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)) engine=Memory;
insert into source format JSONAsObject
{"a" : 1, "b" : 10}
{"b" : 20, "c" : 30}
{}
{"a" : 2, "c" : 40}
{"a" : 3}
{};

-- Incompatible non-null values fail insertion (DEFAULT PATH TYPE is a contract).
insert into source select cast('{"a" : "not_a_number"}' as JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)); -- {serverError INCORRECT_DATA}
insert into source select cast('{"a" : [1,2]}' as JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)); -- {serverError INCORRECT_DATA}
-- But with type_json_skip_invalid_typed_paths they are skipped (the path becomes missing).
insert into source settings type_json_skip_invalid_typed_paths=1 values ('{"a" : "not_a_number", "b" : 50}');
-- JSON null is normalized to a missing path.
insert into source values ('{"a" : null, "z" : 60}');

select json from source order by json.a.:`Int64` nulls last, json.b.:`Int64` nulls last, json.c.:`Int64` nulls last, json.z.:`Int64` nulls last settings allow_suspicious_types_in_order_by=1;
drop table source;

-- Values are coerced to the DEFAULT PATH TYPE at insert (not stored with their natural JSON type),
-- so mixed numeric types in the same path are normalized (1 -> 1.0 in Float64, 2.5 stays 2.5).
drop table if exists test_coerce;
create table test_coerce (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Float64)) engine=MergeTree order by tuple();
insert into test_coerce values ('{"a" : 1}'), ('{"a" : 2.5}'), ('{}');
select json, json.a from test_coerce;
optimize table test_coerce final;
select json from test_coerce;
drop table test_coerce;

-- String default path type.
drop table if exists test_string;
create table test_string (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE String)) engine=MergeTree order by tuple();
insert into test_string values ('{"a" : "x", "b" : "y"}'), ('{"a" : "z"}'), ('{}'), ('{"c" : "w"}');
select json, json.a, json.b from test_string;
optimize table test_string final;
select json from test_string;
drop table test_string;

-- Type-hint subcolumns on dynamic paths: a hint equal to the DEFAULT PATH TYPE (as bare T or
-- Nullable(T)) is redundant and stripped; the result type is T (the declared default path type).
drop table if exists test_hints;
create table test_hints (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)) engine=MergeTree order by tuple();
insert into test_hints values ('{"a" : 1}'), ('{}'), ('{"a" : 3}');
select toTypeName(json.a.:`Int64`) from test_hints limit 1;
select toTypeName(json.a.:`Nullable(Int64)`) from test_hints limit 1;
select json.a.:`Int64`, json.a.:`Nullable(Int64)` from test_hints;
drop table test_hints;

-- Same without DEFAULT PATH TYPE: result type is also Nullable(T).
drop table if exists test_hints_no_default;
create table test_hints_no_default (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple();
insert into test_hints_no_default values ('{"a" : 1}'), ('{}'), ('{"a" : 3}');
select toTypeName(json.a.:`Int64`) from test_hints_no_default limit 1;
select json.a.:`Int64` from test_hints_no_default;
drop table test_hints_no_default;

-- With dynamic paths enabled, values are also coerced to the DEFAULT PATH TYPE (not stored
-- with their natural JSON type): the subcolumn has the declared type T.
drop table if exists test_dynamic;
create table test_dynamic (json JSON(DEFAULT PATH TYPE Float64, max_dynamic_paths=10)) engine=MergeTree order by tuple();
insert into test_dynamic values ('{"a" : 1}'), ('{"b" : 2.5}'), ('{}');
select json, toTypeName(json.a), toTypeName(json.b) from test_dynamic;
optimize table test_dynamic final;
select json, toTypeName(json.a), toTypeName(json.b) from test_dynamic;
drop table test_dynamic;

-- Nested objects inherit the DEFAULT PATH TYPE: values at any depth are coerced/validated.
drop table if exists test_nested;
create table test_nested (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)) engine=MergeTree order by tuple();
insert into test_nested values ('{"a" : {"b" : 1}}'), ('{"a" : {"b" : 2, "c" : 3}}'), ('{}');
select json, json.^`a` from test_nested;
insert into test_nested select cast('{"a" : {"b" : "not_a_number"}}' as JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)); -- {serverError INCORRECT_DATA}
optimize table test_nested final;
select json from test_nested;
drop table test_nested;

-- Sub-object subcolumns carry the modifier (json.^a is a JSON with DEFAULT PATH TYPE).
drop table if exists test_subobject_type;
create table test_subobject_type (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)) engine=MergeTree order by tuple();
insert into test_subobject_type values ('{"a" : {"b" : 1}}');
select toTypeName(json.^`a`) from test_subobject_type;
drop table test_subobject_type;

-- getLeastSupertype: JSON with different DEFAULT PATH TYPE falls back to a type without it.
drop table if exists test_supertype;
select toTypeName(if(dummy, CAST('{"a":1}' as JSON(DEFAULT PATH TYPE Int64)), CAST('{"a":1}' as JSON(DEFAULT PATH TYPE String)))) settings allow_suspicious_types_in_order_by=1;

-- Typed paths are still typed paths (not coerced by DEFAULT PATH TYPE).
drop table if exists test_typed_vs_default;
create table test_typed_vs_default (json JSON(DEFAULT PATH TYPE Int64, s String)) engine=MergeTree order by tuple();
insert into test_typed_vs_default values ('{"s" : "text", "a" : 5}');
select json, json.s, json.a, toTypeName(json.s) from test_typed_vs_default;
drop table test_typed_vs_default;

-- max_dynamic_types=0 forces dynamic-path values into the shared variant; they are still coerced to T.
drop table if exists test_shared_variant;
create table test_shared_variant (json JSON(DEFAULT PATH TYPE Int64, max_dynamic_paths=10, max_dynamic_types=0)) engine=MergeTree order by tuple();
insert into test_shared_variant values ('{"a" : 1}'), ('{"b" : 2}');
select json, toTypeName(json.a), toTypeName(json.b) from test_shared_variant;
drop table test_shared_variant;

-- Nullable default path type: a valid value is coerced to Nullable(T). JSON null is normalized to a missing path.
drop table if exists test_nullable_dpt;
create table test_nullable_dpt (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Nullable(Int64))) engine=MergeTree order by tuple();
insert into test_nullable_dpt values ('{"a" : 1}'), ('{}'), ('{"b" : 5}'), ('{"a" : null}');
select json, json.a, json.b from test_nullable_dpt;
optimize table test_nullable_dpt final;
select json from test_nullable_dpt;
drop table test_nullable_dpt;

-- Nullable default path type with dynamic paths: the subcolumn has type Nullable(T).
drop table if exists test_nullable_dyn;
create table test_nullable_dyn (json JSON(DEFAULT PATH TYPE Nullable(Int64), max_dynamic_paths=10)) engine=MergeTree order by tuple();
insert into test_nullable_dyn values ('{"a" : 1}'), ('{"b" : 2}'), ('{}');
select json, toTypeName(json.a), toTypeName(json.b) from test_nullable_dyn;
optimize table test_nullable_dyn final;
select json, toTypeName(json.a), toTypeName(json.b) from test_nullable_dyn;
drop table test_nullable_dyn;

-- Dynamic default path type: values are stored as-is (no coercion).
drop table if exists test_dynamic_dpt;
create table test_dynamic_dpt (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Dynamic)) engine=MergeTree order by tuple();
insert into test_dynamic_dpt values ('{"a" : 1}'), ('{"b" : "x"}'), ('{}');
select json, json.a, json.b from test_dynamic_dpt;
optimize table test_dynamic_dpt final;
select json from test_dynamic_dpt;
drop table test_dynamic_dpt;

-- Array default path type.
drop table if exists test_array_dpt;
create table test_array_dpt (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Array(Int64))) engine=MergeTree order by tuple();
insert into test_array_dpt values ('{"a" : [1,2,3]}'), ('{"b" : []}'), ('{}');
select json, json.a, json.b from test_array_dpt;
insert into test_array_dpt select cast('{"a" : "not_an_array"}' as JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Array(Int64))); -- {serverError INCORRECT_DATA}
select count() from test_array_dpt;
drop table test_array_dpt;

-- Dynamic paths (max_dynamic_paths > 0) with shared-data overflow: values coerced to T everywhere,
-- read back as T from both dynamic paths and shared data.
drop table if exists test_dyn_overflow;
create table test_dyn_overflow (json JSON(DEFAULT PATH TYPE Int64, max_dynamic_paths=2)) engine=MergeTree order by tuple();
insert into test_dyn_overflow values ('{"a":1,"b":2}'), ('{"a":3,"c":4}'), ('{"d":5,"e":6,"f":7}'), ('{}');
select json, json.a, json.c, json.f, toTypeName(json.f) from test_dyn_overflow;
optimize table test_dyn_overflow final;
select json, json.a, json.c, json.f from test_dyn_overflow;
drop table test_dyn_overflow;

-- Sub-object subcolumn carries the modifier and reads as T.
drop table if exists test_subobj_read;
create table test_subobj_read (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)) engine=MergeTree order by tuple();
insert into test_subobj_read values ('{"a":{"b":1,"c":2}}'), ('{"a":{"b":3}}'), ('{}');
select json, json.^`a`, json.a.b.c from test_subobj_read;
optimize table test_subobj_read final;
select json.^`a`, json.a.b.c from test_subobj_read;
drop table test_subobj_read;

-- Combined subcolumn (json.@a): literal (T) and sub-object merged into Dynamic.
drop table if exists test_combined;
create table test_combined (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)) engine=MergeTree order by tuple();
insert into test_combined values ('{"a":5}'), ('{"a":{"b":6}}'), ('{}');
select json.@`a`, toTypeName(json.@`a`) from test_combined;
optimize table test_combined final;
select json.@`a` from test_combined;
drop table test_combined;

-- Changing DEFAULT PATH TYPE is safe: old values keep their encoded type, new values use the new type.
drop table if exists test_alter_dpt;
create table test_alter_dpt (json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE Int64)) engine=MergeTree order by tuple();
insert into test_alter_dpt values ('{"a":5}'), ('{}');
alter table test_alter_dpt modify column json JSON(max_dynamic_paths=0, DEFAULT PATH TYPE String);
insert into test_alter_dpt values ('{"a":"hello"}');
select json, json.a, toTypeName(json.a) from test_alter_dpt order by json.a;
optimize table test_alter_dpt final;
select json, json.a from test_alter_dpt order by json.a;
drop table test_alter_dpt;

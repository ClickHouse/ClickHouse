set allow_experimental_variant_type=1;
set use_variant_as_common_type=1;

select toTypeName(res), if(1, [1,2,3], 'str_1') as res;
select toTypeName(res), if(1, [1,2,3], 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(0, [1,2,3], 'str_1') as res;
select toTypeName(res), if(0, [1,2,3], 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(NULL, [1,2,3], 'str_1') as res;
select toTypeName(res), if(NULL, [1,2,3], 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), [1,2,3], 'str_1') as res;
select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), [1,2,3], 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(1, materialize([1,2,3]), 'str_1') as res;
select toTypeName(res), if(1, materialize([1,2,3]), 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(0, materialize([1,2,3]), 'str_1') as res;
select toTypeName(res), if(0, materialize([1,2,3]), 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(NULL, materialize([1,2,3]), 'str_1') as res;
select toTypeName(res), if(NULL, materialize([1,2,3]), 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), materialize([1,2,3]), 'str_1') as res;
select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), materialize([1,2,3]), 'str_1'::Nullable(String)) as res;

select toTypeName(res), if(1, [1,2,3], materialize('str_1')) as res;
select toTypeName(res), if(1, [1,2,3], materialize('str_1')::Nullable(String)) as res;

select toTypeName(res), if(0, [1,2,3], materialize('str_1')) as res;
select toTypeName(res), if(0, [1,2,3], materialize('str_1')::Nullable(String)) as res;

select toTypeName(res), if(NULL, [1,2,3], materialize('str_1')) as res;
select toTypeName(res), if(NULL, [1,2,3], materialize('str_1')::Nullable(String)) as res;

select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), [1,2,3], materialize('str_1')) as res;
select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), [1,2,3], materialize('str_1')::Nullable(String)) as res;


select toTypeName(res), if(0, range(number + 1), 'str_' || toString(number)) as res from numbers(4);
select toTypeName(res), if(0, range(number + 1), ('str_' || toString(number))::Nullable(String)) as res from numbers(4);

select toTypeName(res), if(1, range(number + 1), 'str_' || toString(number)) as res from numbers(4);
select toTypeName(res), if(1, range(number + 1), ('str_' || toString(number))::Nullable(String)) as res from numbers(4);

select toTypeName(res), if(NULL, range(number + 1), 'str_' || toString(number)) as res from numbers(4);
select toTypeName(res), if(NULL, range(number + 1), ('str_' || toString(number))::Nullable(String)) as res from numbers(4);

select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), range(number + 1), 'str_' || toString(number)) as res from numbers(4);
select toTypeName(res), if(materialize(NULL::Nullable(UInt8)), range(number + 1), ('str_' || toString(number))::Nullable(String)) as res from numbers(4);

select toTypeName(res), if(number % 2, range(number + 1), 'str_' || toString(number)) as res from numbers(4);
select toTypeName(res), if(number % 2, range(number + 1), ('str_' || toString(number))::Nullable(String)) as res from numbers(4);

select toTypeName(res), if(number % 2, range(number + 1), ('str_' || toString(number))::LowCardinality(String)) as res from numbers(4);
select toTypeName(res), if(number % 2, range(number + 1), ('str_' || toString(number))::LowCardinality(Nullable(String))) as res from numbers(4);


select toTypeName(res), multiIf(number % 3 == 0, range(number + 1), number % 3 == 1, number, 'str_' || toString(number)) as res from numbers(6);
select toTypeName(res), multiIf(number % 3 == 0, range(number + 1), number % 3 == 1, number,  ('str_' || toString(number))::Nullable(String)) as res from numbers(6);
select toTypeName(res), multiIf(number % 3 == 0, range(number + 1), number % 3 == 1, number, ('str_' || toString(number))::LowCardinality(String)) as res from numbers(6);
select toTypeName(res), multiIf(number % 3 == 0, range(number + 1), number % 3 == 1, number, ('str_' || toString(number))::LowCardinality(Nullable(String))) as res from numbers(6);


select toTypeName(res), array(1, 'str_1', 2, 'str_2') as res;
select toTypeName(res), array([1, 2, 3], ['str_1', 'str_2', 'str_3']) as res;
select toTypeName(res), array(array([1, 2, 3], ['str_1', 'str_2', 'str_3']), [1, 2, 3]) as res;
select toTypeName(res), array([1, 2, 3], [[1, 2, 3]]) as res;

select toTypeName(res), map('a', 1, 'b', 'str_1') as res;
select toTypeName(res), map('a', 1, 'b', map('c', 2, 'd', 'str_1')) as res;
select toTypeName(res), map('a', 1, 'b', [1, 2, 3], 'c', [[4, 5, 6]]) as res;

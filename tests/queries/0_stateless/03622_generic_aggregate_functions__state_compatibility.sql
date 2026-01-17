--- Some aggregate functions use IColumn::serializeAggregationStateValueIntoArena
--- method for values serialization into aggregation states.
--- Aggregation states should not be changed for compatibility,
--- and this test should fail if something is changed in serializeAggregationStateValueIntoArena method.

set session_timezone='UTC';

select hex(approx_top_sumState(3)(['str'], 1));
select hex(approx_top_kState(3)(['str']));
select hex(topKState(3)(['str']));
select hex(topKWeightedState(3)(['str'], 1));
select hex(maxDistinctState(['str']));
select hex(argMaxDistinctState(['str'], ['str']));
select hex(groupUniqArrayState(['str']));
select hex(groupArrayIntersectState(['str']));

--- Check different data types
select hex(maxDistinctState(tuple('str', true)));
select hex(maxDistinctState(tuple('str', 42::Int8)));
select hex(maxDistinctState(tuple('str', 42::UInt8)));
select hex(maxDistinctState(tuple('str', 42::Int16)));
select hex(maxDistinctState(tuple('str', 42::UInt16)));
select hex(maxDistinctState(tuple('str', 42::Int32)));
select hex(maxDistinctState(tuple('str', 42::UInt32)));
select hex(maxDistinctState(tuple('str', 42::Int64)));
select hex(maxDistinctState(tuple('str', 42::UInt64)));
select hex(maxDistinctState(tuple('str', 42::Int128)));
select hex(maxDistinctState(tuple('str', 42::UInt128)));
select hex(maxDistinctState(tuple('str', 42::Int256)));
select hex(maxDistinctState(tuple('str', 42::UInt256)));
select hex(maxDistinctState(tuple('str', 42.42::BFloat16)));
select hex(maxDistinctState(tuple('str', 42.42::Float32)));
select hex(maxDistinctState(tuple('str', 42.42::Float64)));
select hex(maxDistinctState(tuple('str', 42.42::Decimal32(2))));
select hex(maxDistinctState(tuple('str', 42.42::Decimal64(2))));
select hex(maxDistinctState(tuple('str', 42.42::Decimal128(2))));
select hex(maxDistinctState(tuple('str', 42.42::Decimal256(2))));
select hex(maxDistinctState(tuple('str', 'str'::Nullable(String))));
select hex(maxDistinctState(tuple('str', 'str'::FixedString(3))));
select hex(maxDistinctState(tuple('str', 'str'::Nullable(FixedString(3)))));
select hex(maxDistinctState(tuple('str', 'str'::LowCardinality(String))));
select hex(maxDistinctState(tuple('str', 'str'::LowCardinality(Nullable(String)))));
select hex(maxDistinctState(tuple('str', 'str'::LowCardinality(FixedString(3)))));
select hex(maxDistinctState(tuple('str', 'str'::LowCardinality(Nullable(FixedString(3))))));
select hex(maxDistinctState(tuple('str', ['str'])));
select hex(maxDistinctState(tuple('str', map('str', 'str'))));
select hex(maxDistinctState(tuple('str', tuple('str'))));
select hex(maxDistinctState(tuple('str', '{"str" : "str"}'::JSON)));
select hex(maxDistinctState(tuple('str', '{"str" : "str"}'::JSON(max_dynamic_paths=0))));
select hex(maxDistinctState(tuple('str', '{"str" : "str"}'::JSON(str String))));
select hex(maxDistinctState(tuple('str', 'str'::Variant(String))));
select hex(maxDistinctState(tuple('str', 'str'::Dynamic)));
select hex(maxDistinctState(tuple('str', 'str'::Dynamic(max_types=0))));
select hex(maxDistinctState(tuple('str', '59cd9014-8730-444c-95d0-40ed67c54268'::UUID)));
select hex(maxDistinctState(tuple('str', '127.0.0.1'::IPv4)));
select hex(maxDistinctState(tuple('str', '127.0.0.1'::IPv6)));
select hex(maxDistinctState(tuple('str', '2020-01-01'::Date)));
select hex(maxDistinctState(tuple('str', '2020-01-01'::Date32)));
select hex(maxDistinctState(tuple('str', '2020-01-01'::DateTime)));
select hex(maxDistinctState(tuple('str', '2020-01-01'::DateTime64)));
select hex(maxDistinctState(tuple('str', 'a'::Enum8('a' = 1))));
select hex(maxDistinctState(tuple('str', 'a'::Enum16('a' = 1))));

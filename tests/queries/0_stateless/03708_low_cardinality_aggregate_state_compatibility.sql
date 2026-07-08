select hex(maxDistinctState(tuple('str'::Variant(LowCardinality(String)))));
select hex(maxDistinctState(tuple(''::Variant(LowCardinality(String)))));
select maxDistinctMerge(state) from (select maxDistinctState(tuple('str'::Variant(LowCardinality(String)))) as state);
select maxDistinctMerge(state) from (select maxDistinctState(tuple(''::Variant(LowCardinality(String)))) as state);


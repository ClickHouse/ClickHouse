select hex(groupUniqArrayState(tuple('str'::Variant(LowCardinality(String)))));
select hex(groupUniqArrayState(tuple(''::Variant(LowCardinality(String)))));
select groupUniqArrayMerge(state) from (select groupUniqArrayState(tuple('str'::Variant(LowCardinality(String)))) as state);
select groupUniqArrayMerge(state) from (select groupUniqArrayState(tuple(''::Variant(LowCardinality(String)))) as state);


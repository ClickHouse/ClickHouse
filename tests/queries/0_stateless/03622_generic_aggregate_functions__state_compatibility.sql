select hex(approx_top_sumState(3)(['str'], 1));
select hex(approx_top_kState(3)(['str']));
select hex(topKState(3)(['str']));
select hex(topKWeightedState(3)(['str'], 1));
select hex(maxDistinctState(['str']));
select hex(argMaxDistinctState(['str'], ['str']));
select hex(groupUniqArrayState(['str']));
select hex(groupArrayIntersectState(['str']));

select hex(approx_top_sumState(3)([toLowCardinality('str')], 1));
select hex(approx_top_kState(3)([toLowCardinality('str')]));
select hex(topKState(3)([toLowCardinality('str')]));
select hex(topKWeightedState(3)([toLowCardinality('str')], 1));
select hex(maxDistinctState([toLowCardinality('str')]));
select hex(argMaxDistinctState([toLowCardinality('str')], [toLowCardinality('str')]));
select hex(groupUniqArrayState([toLowCardinality('str')]));
select hex(groupArrayIntersectState([toLowCardinality('str')]))

-- Regression for `groupFormat` aggregate-state serialization (PR #93201).
-- `finalizeAggregation(groupFormatState(...))` finalizes the in-memory state and does NOT
-- exercise the custom binary `serialize` / `deserialize`. Storing the state in a `MergeTree`
-- `AggregateFunction` column forces `serialize` on INSERT (into the on-disk part) and
-- `deserialize` on read, so this proves the binary state format round-trips.

set output_format_write_statistics = 0;
set max_threads = 1;

drop table if exists t_group_format_state;

create table t_group_format_state (state AggregateFunction(groupFormat('JSONEachRow'), UInt64))
engine = MergeTree order by tuple();

insert into t_group_format_state select groupFormatState('JSONEachRow')(number) from numbers(3);

-- The state is read back from the on-disk part (deserialize) and finalized.
select finalizeAggregation(state) from t_group_format_state;

-- The stored-then-deserialized result must equal direct in-memory aggregation.
select (select finalizeAggregation(state) from t_group_format_state) = groupFormat('JSONEachRow')(number) from numbers(3);

drop table t_group_format_state;

-- Merge of states deserialized from two separate parts (order-independent presence check).
drop table if exists t_group_format_merge;

create table t_group_format_merge (k UInt64, state AggregateFunction(groupFormat('JSONEachRow'), UInt64))
engine = MergeTree order by k;

system stop merges t_group_format_merge;
insert into t_group_format_merge select 1, groupFormatState('JSONEachRow')(number) from numbers(2);
insert into t_group_format_merge select 1, groupFormatState('JSONEachRow')(number + 2) from numbers(2);

select
    position(result, '{"c1":0}') > 0 and position(result, '{"c1":1}') > 0 and
    position(result, '{"c1":2}') > 0 and position(result, '{"c1":3}') > 0
from (select groupFormatMerge('JSONEachRow')(state) as result from t_group_format_merge group by k);

drop table t_group_format_merge;

-- A `Dynamic` payload exercises the self-describing Native serialization version (V1/V2):
-- the writer embeds the chosen version into the stream and the reader recovers it from the
-- stream, so the round-trip is lossless regardless of the captured client protocol revision.
drop table if exists t_group_format_dynamic;

create table t_group_format_dynamic (state AggregateFunction(groupFormat('JSONEachRow'), Dynamic))
engine = MergeTree order by tuple();

insert into t_group_format_dynamic
    select groupFormatState('JSONEachRow')(d)
    from (select arrayJoin([42::Int64, 'hi'::String, 2.5::Float64]::Array(Dynamic)) as d);

select finalizeAggregation(state) from t_group_format_dynamic;

select (select finalizeAggregation(state) from t_group_format_dynamic) =
       (select groupFormat('JSONEachRow')(d) from (select arrayJoin([42::Int64, 'hi'::String, 2.5::Float64]::Array(Dynamic)) as d));

drop table t_group_format_dynamic;

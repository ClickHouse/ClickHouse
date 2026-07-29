#include <Storages/MergeTree/RuntimeFilterReadRangesRefiner.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>

namespace DB
{

RuntimeFilterReadRangesRefiner::RuntimeFilterReadRangesRefiner(
    StorageMetadataPtr metadata_snapshot_, ContextPtr context_, String key_column_name_)
    : metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(context_))
    , key_column_name(std::move(key_column_name_))
{
}

RuntimeFilterReadRangesRefiner::~RuntimeFilterReadRangesRefiner() = default;

void RuntimeFilterReadRangesRefiner::setFilter(const RuntimeFilterConstPtr & filter)
{
    std::call_once(set_filter_once, [&] { setFilterImpl(filter); });
}

void RuntimeFilterReadRangesRefiner::setFilterImpl(const RuntimeFilterConstPtr & filter)
{
    chassert(filter && filter->isReady());

    auto values = filter->getRecordedKeyValues();
    if (!values)
        return; /// No exact values (e.g. bloom overflow): only the row-level filter applies.

    if (values->empty())
    {
        /// The build side has no keys: nothing on the probe side can match.
        drop_all = true;
        return;
    }

    /// Build `key IN (recorded values)` and turn it into a primary key condition.
    const auto & target_type = filter->getFilterColumnTargetType();
    auto key_type = metadata_snapshot->getColumns().getPhysical(key_column_name).type;

    ActionsDAG dag;
    const auto * key_node = &dag.addInput(key_column_name, key_type);
    if (!key_type->equals(*target_type))
        key_node = &dag.addCast(*key_node, target_type, {}, context);

    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{},
        nullptr,
        ColumnsWithTypeAndName{ColumnWithTypeAndName(values, target_type, "")},
        /*transform_null_in=*/false,
        SizeLimits{});
    auto set_column = ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0);
    const auto & set_node = dag.addColumn(std::move(set_column), std::make_shared<DataTypeSet>(), "__runtime_filter_set");

    auto in_function = FunctionFactory::instance().get("in", context);
    const auto & in_node = dag.addFunction(in_function, {key_node, &set_node}, {});
    dag.getOutputs() = {&in_node};

    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    ActionsDAGWithInversionPushDown inverted_dag(&in_node, context, /*boolean_context=*/true);
    condition = std::make_shared<const KeyCondition>(inverted_dag, context, primary_key.column_names, primary_key.expression);
}

MarkRanges RuntimeFilterReadRangesRefiner::refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const
{
    if (drop_all)
        return {};

    if (!condition)
        return ranges;

    RangesInDataPart part(info.data_part, info.parent_part, info.part_index_in_query, info.part_starting_offset_in_query);
    part.ranges = std::move(ranges);

    return MergeTreeDataSelectExecutor::markRangesFromPKRange(
        part,
        metadata_snapshot,
        *condition,
        /*part_offset_condition=*/nullptr,
        /*total_offset_condition=*/nullptr,
        /*exact_ranges=*/nullptr,
        /*pk_to_minmax_slot=*/nullptr,
        context->getSettingsRef(),
        getLogger("RuntimeFilterReadRangesRefiner"));
}

}

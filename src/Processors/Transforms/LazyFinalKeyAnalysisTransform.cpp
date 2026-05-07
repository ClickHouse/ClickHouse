#include <Analyzer/TableExpressionModifiers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Set.h>
#include <Processors/Port.h>
#include <Processors/Transforms/LazyFinalKeyAnalysisTransform.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsNonZeroUInt64 max_block_size;
}

LazyFinalKeyAnalysisTransform::LazyFinalKeyAnalysisTransform(
    FutureSetPtr future_set_,
    LazyFinalSharedStatePtr shared_state_,
    StorageMetadataPtr metadata_snapshot_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeSettingsPtr data_settings_,
    const MergeTreeData & data_,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
    RangesInDataPartsPtr ranges_,
    ContextPtr query_context_,
    float min_filtered_ratio_)
    : IProcessor(InputPorts{InputPort(Block())}, OutputPorts{OutputPort(Block())})
    , future_set(std::move(future_set_))
    , shared_state(std::move(shared_state_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , data_settings(std::move(data_settings_))
    , data(data_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , ranges(std::move(ranges_))
    , query_context(std::move(query_context_))
    , min_filtered_ratio(min_filtered_ratio_)
    , log(getLogger("LazyFinalKeyAnalysisTransform"))
{
}

IProcessor::Status LazyFinalKeyAnalysisTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (input.isFinished())
    {
        auto set = future_set->get();
        if (!set)
        {
            LOG_TRACE(log, "Lazy FINAL disabled: set is not available");
        }
        else if (set->isTruncated())
        {
            LOG_TRACE(log,
                "Lazy FINAL disabled: set was truncated (rows={}, bytes={}, max_rows={}, max_bytes={})",
                set->getTotalRowCount(), set->getTotalByteCount(),
                set->limits.max_rows, set->limits.max_bytes);
        }
        else
        {
            if (!is_done)
                return Status::Ready;

            if (should_signal)
                output.push(Chunk());
        }

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    input.pull();
    return Status::NeedData;
}

std::unique_ptr<ReadFromMergeTree> LazyFinalKeyAnalysisTransform::buildReadingStep(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeSettingsPtr & data_settings,
    const MergeTreeData & data,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read,
    RangesInDataPartsPtr ranges,
    ContextPtr query_context)
{
    const auto & settings = query_context->getSettingsRef();
    const auto & sorting_key = metadata_snapshot->getSortingKey();
    const auto & merging_params = data.merging_params;

    Names all_column_names;
    std::unordered_set<std::string_view> columns_to_read;
    for (const auto & column : sorting_key.expression->getRequiredColumnsWithTypes())
    {
        columns_to_read.insert(column.name);
        all_column_names.push_back(column.name);
    }
    if (!merging_params.version_column.empty() && !columns_to_read.contains(merging_params.version_column))
    {
        columns_to_read.insert(merging_params.version_column);
        all_column_names.push_back(merging_params.version_column);
    }
    if (!merging_params.is_deleted_column.empty() && !columns_to_read.contains(merging_params.is_deleted_column))
    {
        columns_to_read.insert(merging_params.is_deleted_column);
        all_column_names.push_back(merging_params.is_deleted_column);
    }

    SelectQueryInfo query_info;
    query_info.table_expression_modifiers = TableExpressionModifiers(false, {}, {});

    auto reading = std::make_unique<ReadFromMergeTree>(
        std::move(ranges),
        mutations_snapshot,
        all_column_names,
        data,
        data_settings,
        query_info,
        storage_snapshot,
        query_context,
        settings[Setting::max_block_size],
        settings[Setting::max_threads],
        max_block_numbers_to_read,
        getLogger("LazyFinalKeyAnalysisTransform"),
        nullptr,
        false);

    reading->disableQueryConditionCache();
    return reading;
}

void LazyFinalKeyAnalysisTransform::work()
{
    auto reading = buildReadingStep(
        metadata_snapshot, mutations_snapshot, storage_snapshot,
        data_settings, data, max_block_numbers_to_read, ranges, query_context);

    /// Count total marks before index analysis.
    size_t total_marks = 0;
    for (const auto & part : reading->getParts())
        total_marks += part.getMarksCount();

    /// Build and apply the IN-set filter for index analysis.
    {
        const auto & primary_key = metadata_snapshot->getPrimaryKey();

        ActionsDAG filter_dag = primary_key.expression->getActionsDAG().clone();
        filter_dag.getOutputs() = filter_dag.findInOutputs(primary_key.column_names);

        ColumnWithTypeAndName column_set;
        column_set.type = std::make_shared<DataTypeSet>();
        column_set.column = ColumnSet::create(0, future_set);

        const auto * key_node = filter_dag.getOutputs().at(0);
        if (filter_dag.getOutputs().size() > 1)
        {
            auto function_tuple = FunctionFactory::instance().get("tuple", query_context);
            key_node = &filter_dag.addFunction(function_tuple, filter_dag.getOutputs(), {});
        }
        const auto * set_node = &filter_dag.addColumn(std::move(column_set));
        auto function_in = FunctionFactory::instance().get("in", query_context);
        const auto * in_func = &filter_dag.addFunction(function_in, {key_node, set_node}, {});
        filter_dag.getOutputs().push_back(in_func);

        reading->addFilter(std::move(filter_dag), in_func->result_name);
        reading->SourceStepWithFilterBase::applyFilters();
    }

    /// Count marks after index analysis.
    auto analysis_result = reading->selectRangesToRead();
    size_t selected_marks = 0;
    if (analysis_result)
        for (const auto & part : analysis_result->parts_with_ranges)
            selected_marks += part.getMarksCount();

    /// Check if enough marks were filtered by the IN-set.
    float filtered_ratio = total_marks > 0 ? 1.0f - static_cast<float>(selected_marks) / static_cast<float>(total_marks) : 0.0f;

    if (min_filtered_ratio > 0 && filtered_ratio < min_filtered_ratio)
    {
        LOG_TRACE(log,
            "Lazy FINAL disabled: filtered ratio {:.2f} is below threshold {:.2f} "
            "(total_marks={}, selected_marks={}, set_rows={})",
            filtered_ratio, min_filtered_ratio,
            total_marks, selected_marks, future_set->get()->getTotalRowCount());
        should_signal = false;
    }
    else
    {
        LOG_TRACE(log,
            "Lazy FINAL enabled: filtered ratio {:.2f} (threshold {:.2f}), "
            "total_marks={}, selected_marks={}, set_rows={}",
            filtered_ratio, min_filtered_ratio,
            total_marks, selected_marks, future_set->get()->getTotalRowCount());
        should_signal = true;
        shared_state->reading_step = std::move(reading);
    }

    is_done = true;
}

}

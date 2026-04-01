#include <Core/Settings.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Transforms/LazyFinalKeyAnalysisTransform.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}

LazyFinalKeyAnalysisTransform::LazyFinalKeyAnalysisTransform(
    FutureSetPtr future_set_,
    ContextPtr query_context_,
    StorageMetadataPtr metadata_snapshot_,
    LazyFinalKeyAnalysisTransform::MutableRangesInDataPartsPtr ranges_)
    : IProcessor(InputPorts{InputPort(Block())}, OutputPorts{OutputPort(Block())})
    , future_set(std::move(future_set_))
    , ranges(std::move(ranges_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , query_context(std::move(query_context_))
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
        return Status::NeedData;

    if (input.isFinished())
    {
        auto set = future_set->get();
        if (set && !set->isTruncated())
        {
            if (!is_done)
                return Status::Ready;

            /// Set was built successfully — signal InputSelectorTransform to use the optimized path.
            output.push(Chunk());
        }

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    /// Discard any signal chunks.
    input.pull();
    return Status::NeedData;
}

void LazyFinalKeyAnalysisTransform::work()
{
    const auto & settings = query_context->getSettingsRef();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();

    ColumnWithTypeAndName column_set;
    column_set.type = std::make_shared<DataTypeSet>();
    column_set.column = ColumnSet::create(0, future_set);

    ActionsDAG dag(primary_key.sample_block.getColumnsWithTypeAndName());

    auto function_tuple = FunctionFactory::instance().get("tuple", query_context);
    auto function_in = FunctionFactory::instance().get("in", query_context);
    const auto * tuple_node = &dag.addFunction(function_tuple, dag.getOutputs(), {});
    const auto * set_node = &dag.addColumn(std::move(column_set));
    const auto * in_func = &dag.addFunction(function_in, {tuple_node, set_node}, {});

    ActionsDAGWithInversionPushDown canonical_filter_dag(in_func, query_context);

    KeyCondition key_condition{
        canonical_filter_dag,
        query_context,
        primary_key.column_names,
        primary_key.expression,
        /* single_point_ = */ false,
        /* skip_analysis_ = */ false};

    ReadFromMergeTree::Indexes indexes(std::move(key_condition));

    ReadFromMergeTree::AnalysisResult analysis_result;
    SelectQueryInfo query_info;
    query_info.table_expression_modifiers = TableExpressionModifiers(false, {}, {});
    MergeTreeReaderSettings reader_settings = MergeTreeReaderSettings::createFromContext(query_context);
    MergeTreeDataSelectExecutor::IndexAnalysisContext filter_context
    {
        .metadata_snapshot = metadata_snapshot,
        .mutations_snapshot = nullptr,
        .query_info = query_info,
        .context = query_context,
        .indexes = indexes,
        .top_k_filter_info = std::nullopt,
        .reader_settings = reader_settings,
        .log = getLogger("LazyFinalKeyAnalysisTransform"),
        .num_streams = settings[Setting::max_threads],
        .find_exact_ranges = false,
        .is_parallel_reading_from_replicas = false,
        .has_projections = false,
        .result = analysis_result,
    };
    *ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(filter_context, std::move(*ranges), analysis_result.index_stats);

    is_done = true;
}

}

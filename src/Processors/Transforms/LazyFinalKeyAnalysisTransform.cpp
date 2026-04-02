#include <Core/Settings.h>
#include <DataTypes/DataTypeSet.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/LazyFinalKeyAnalysisTransform.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{

static std::vector<size_t> getKeyColumnPositions(const StorageMetadataPtr & metadata_snapshot, const Block & header)
{
    const auto & key_column_names = metadata_snapshot->getPrimaryKey().column_names;
    std::vector<size_t> positions;
    positions.reserve(key_column_names.size());
    for (const auto & name : key_column_names)
        positions.push_back(header.getPositionByName(name));
    return positions;
}

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}

LazyFinalKeyAnalysisTransform::LazyFinalKeyAnalysisTransform(
    const Block & header,
    size_t max_rows_,
    ContextPtr query_context_,
    StorageMetadataPtr metadata_snapshot_,
    RangesInDataParts ranges_)
    : IProcessor(InputPorts{InputPort(header)}, OutputPorts{OutputPort(Block())})
    , max_rows(max_rows_)
    , key_columns(getKeyColumnPositions(metadata_snapshot, header))
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
        chunks.clear();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::NeedData;

    if (input.isFinished())
    {
        if (!pk_is_analyzed)
            return Status::Ready;

        output.push(Chunk());
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    auto chunk = input.pull();
    rows_read += chunk.getNumRows();

    if (rows_read > max_rows)
    {
        LOG_TRACE(getLogger("LazyFinalKeyAnalysisTransform"),
            "Read more than {} rows, fallback to ordinary FINAL execution.", max_rows);

        chunks.clear();
        output.finish();
        return Status::Finished;
    }

    size_t num_rows = chunk.getNumRows();
    if (num_rows)
    {
        auto columns = chunk.detachColumns();
        Columns filtered_columns;
        filtered_columns.reserve(key_columns.size());
        for (size_t i : key_columns)
            filtered_columns.push_back(std::move(columns[i]));
        chunks.push_back(Chunk(std::move(filtered_columns), num_rows));
    }
    return Status::NeedData;
}

void LazyFinalKeyAnalysisTransform::work()
{
    const auto & settings = query_context->getSettingsRef();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    SizeLimits no_limits;

    auto source = std::make_shared<SourceFromChunks>(std::make_shared<const Block>(primary_key.sample_block), std::move(chunks));
    Pipe pipe(std::move(source));
    auto source_step = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
    auto plan = std::make_unique<QueryPlan>();
    plan->addStep(std::move(source_step));

    auto future_set = std::make_shared<FutureSetFromSubquery>(
        FutureSet::Hash{}, nullptr, std::move(plan),
        nullptr, nullptr, /*transform_null_in*/ false, no_limits, /*max_size_for_index*/ 0);

    ColumnWithTypeAndName column_set;
    column_set.type = std::make_shared<DataTypeSet>();
    column_set.column = ColumnSet::create(0, std::move(future_set));

    // Build and check if primary key is used when necessary
    const Names & primary_key_column_names = primary_key.column_names;

    ActionsDAG dag(primary_key.sample_block.getColumnsWithTypeAndName());

    auto function_tuple = FunctionFactory::instance().get("tuple", query_context);
    auto function_in = FunctionFactory::instance().get("in", query_context);
    const auto * tuple_node = &dag.addFunction(function_tuple, dag.getOutputs(), {});
    const auto * set_node = &dag.addColumn(std::move(column_set));
    const auto * in_func = &dag.addFunction(function_in, {tuple_node, set_node}, {});
    // dag.getOutputs() = {in_func};

    ActionsDAGWithInversionPushDown canonical_filter_dag(in_func, query_context);

    KeyCondition key_condition{
        canonical_filter_dag,
        query_context,
        primary_key_column_names,
        primary_key.expression,
        /* single_point_ = */ false,
        /* skip_analysis_ = */ false};

    ReadFromMergeTree::Indexes indexes(std::move(key_condition));

    ReadFromMergeTree::AnalysisResult analysis_result;
    SelectQueryInfo query_info;
    MergeTreeReaderSettings reader_settings = MergeTreeReaderSettings::createFromContext(query_context);
    MergeTreeDataSelectExecutor::IndexAnalysisContext filter_context
    {
        .metadata_snapshot = metadata_snapshot,
        .mutations_snapshot = nullptr, //snapshot_data.mutations_snapshot,
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
    ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(filter_context, std::move(ranges), analysis_result.index_stats);

    pk_is_analyzed = true;
}

}

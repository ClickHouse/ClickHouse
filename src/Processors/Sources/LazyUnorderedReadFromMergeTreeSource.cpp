#include <Processors/Port.h>
#include <Processors/Sources/LazyUnorderedReadFromMergeTreeSource.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LazyUnorderedReadFromMergeTreeSource::LazyUnorderedReadFromMergeTreeSource(
    SharedHeader header,
    size_t max_block_size_,
    size_t max_threads_,
    MergeTreeReaderSettings reader_settings_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    const MergeTreeData & data_,
    ContextPtr context_,
    const std::string & log_name_,
    LazyMaterializingRowsPtr lazy_materializing_rows_)
    : IProcessor({}, {std::move(header)})
    , max_block_size(max_block_size_)
    , max_threads(max_threads_)
    , reader_settings(reader_settings_)
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , data(data_)
    , context(std::move(context_))
    , log_name(log_name_)
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
}

IProcessor::Status LazyUnorderedReadFromMergeTreeSource::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (lazy_materializing_rows)
        return Status::UpdatePipeline;

    /// Pass through chunks from any ready input.
    bool all_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;

        all_finished = false;
        input.setNeeded();
        if (input.hasData())
        {
            output.push(input.pull());
            return Status::PortFull;
        }
    }

    if (all_finished)
    {
        output.finish();
        return Status::Finished;
    }

    return Status::NeedData;
}

IProcessor::PipelineUpdate LazyUnorderedReadFromMergeTreeSource::updatePipeline()
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyUnorderedReadFromMergeTreeSource: No lazy materializing rows");

    auto readers = buildReaders();
    lazy_materializing_rows.reset();

    for (auto & processor : readers)
    {
        auto & proc_output = processor->getOutputs().front();
        inputs.emplace_back(proc_output.getHeader(), this);
        connect(proc_output, inputs.back());
        inputs.back().setNeeded();
    }

    return PipelineUpdate{.to_add = std::move(readers), .to_remove = {}};
}

Processors LazyUnorderedReadFromMergeTreeSource::buildReaders()
{
    auto & parts = lazy_materializing_rows->ranges_in_data_parts;

    Names column_names;
    for (const auto & col : outputs.front().getHeader())
        column_names.push_back(col.name);

    SelectQueryInfo query_info;
    query_info.table_expression_modifiers = TableExpressionModifiers(false, {}, {});

    auto reading = std::make_unique<ReadFromMergeTree>(
        std::make_shared<RangesInDataParts>(std::move(parts)),
        mutations_snapshot,
        column_names,
        data,
        data.getSettings(),
        query_info,
        storage_snapshot,
        context,
        max_block_size,
        max_threads,
        nullptr,
        getLogger(log_name),
        nullptr,
        false);

    reading->setLazyMaterializingRows(lazy_materializing_rows);
    reading->disableQueryConditionCache();

    QueryPipelineBuilder pipeline;
    reading->initializePipeline(pipeline, BuildQueryPipelineSettings(context));

    auto pipe = QueryPipelineBuilder::getPipe(std::move(pipeline), resources);

    return Pipe::detachProcessors(std::move(pipe));
}

}

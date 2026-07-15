#include <Processors/Port.h>
#include <Processors/QueryPlan/LazilyUnorderedReadFromMergeTree.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Sources/LazyUnorderedReadFromMergeTreeSource.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LazilyUnorderedReadFromMergeTree::LazilyUnorderedReadFromMergeTree(
    SharedHeader header,
    size_t max_block_size_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    const MergeTreeData & data_,
    ContextPtr context_,
    const std::string & log_name_)
    : ISourceStep(std::move(header))
    , max_block_size(max_block_size_)
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , data(data_)
    , context(std::move(context_))
    , log_name(log_name_)
{
}

void LazilyUnorderedReadFromMergeTree::setLazyMaterializingRows(LazyMaterializingRowsPtr lazy_materializing_rows_)
{
    lazy_materializing_rows = std::move(lazy_materializing_rows_);
}

void LazilyUnorderedReadFromMergeTree::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazilyUnorderedReadFromMergeTree: lazy_materializing_rows is not set");

    auto source = std::make_shared<LazyUnorderedReadFromMergeTreeSource>(
        getOutputHeader(),
        max_block_size,
        settings.max_threads,
        MergeTreeReaderSettings::createFromContext(context),
        mutations_snapshot,
        storage_snapshot,
        data,
        context,
        log_name,
        lazy_materializing_rows);

    processors.emplace_back(source);
    pipeline.init(Pipe(std::move(source)));
}

void LazilyUnorderedReadFromMergeTree::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Lazily read columns: ";

    bool first = true;
    for (const auto & column : *getOutputHeader())
    {
        if (!first)
            settings.out << ", ";
        first = false;
        settings.out << column.name;
    }
    settings.out << '\n';
}

void LazilyUnorderedReadFromMergeTree::describeActions(JSONBuilder::JSONMap & map) const
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : *getOutputHeader())
        json_array->add(column.name);
    map.add("Lazily read columns", std::move(json_array));
}

}

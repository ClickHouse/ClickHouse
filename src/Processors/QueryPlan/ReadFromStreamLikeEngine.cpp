#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>

#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool stream_like_engine_allow_direct_select;
}

namespace ErrorCodes
{
extern const int QUERY_NOT_ALLOWED;
}

ReadFromStreamLikeEngine::ReadFromStreamLikeEngine(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::shared_ptr<const StorageLimitsList> storage_limits_,
    ContextPtr context_)
    : ISourceStep{storage_snapshot_->getSampleBlockForColumns(column_names_)}
    , WithContext{context_}
    , storage_limits{std::move(storage_limits_)}
{
}

void ReadFromStreamLikeEngine::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!getContext()->getSettingsRef()[Setting::stream_like_engine_allow_direct_select])
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

    auto pipe = makePipe();

    /// Add storage limits.
    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(storage_limits);

    /// Add to processors to get processor info through explain pipeline statement.
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}
}

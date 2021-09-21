#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/QueryViewsLog.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Chain.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/IStorage.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Common/Stopwatch.h>

namespace Poco
{
class Logger;
}

namespace DB
{

struct ViewRuntimeData
{
    const ASTPtr query;
    Block sample_block;

    StorageID table_id;
    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;

    ContextPtr context;

    std::exception_ptr exception;
    std::unique_ptr<QueryViewsLogElement::ViewRuntimeStats> runtime_stats;

    void setException(std::exception_ptr e)
    {
        exception = e;
        runtime_stats->setStatus(QueryViewsLogElement::ViewStatus::EXCEPTION_WHILE_PROCESSING);
    }
};

/** Writes data to the specified table and to all dependent materialized views.
  */
Chain buildPushingToViewsDrain(
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const ASTPtr & query_ptr,
    bool no_destination,
    ThreadStatus * thread_status,
    std::atomic_uint64_t * elapsed_counter_ms,
    const Block & live_view_header = {});


class ExecutingInnerQueryFromViewTransform final : public ExceptionKeepingTransform
{
public:
    ExecutingInnerQueryFromViewTransform(
        const Block & header,
        ViewRuntimeData & view_data,
        const StorageID & source_storage_id_,
        const StorageMetadataPtr & source_metadata_snapshot_,
        const StoragePtr & source_storage_)
        : ExceptionKeepingTransform(header, view_data.sample_block)
        , view(view_data)
        , source_storage_id(source_storage_id_)
        , source_metadata_snapshot(source_metadata_snapshot_)
        , source_storage(source_storage_)
    {
    }

    String getName() const override { return "ExecutingInnerQueryFromView"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ViewRuntimeData & view;
    const StorageID & source_storage_id;
    const StorageMetadataPtr & source_metadata_snapshot;
    const StoragePtr & source_storage;
};

}

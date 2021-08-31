#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/QueryViewsLog.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Common/Stopwatch.h>
#include <Processors/Drain.h>
#include <Processors/ISimpleTransform.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class ReplicatedMergeTreeSink;

struct ViewRuntimeData
{
    const ASTPtr query;
    Block sample_block;

    StorageID table_id;
    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;

    ContextPtr context;

    std::exception_ptr exception;
    QueryViewsLogElement::ViewRuntimeStats runtime_stats;

    void setException(std::exception_ptr e)
    {
        exception = e;
        runtime_stats.setStatus(QueryViewsLogElement::ViewStatus::EXCEPTION_WHILE_PROCESSING);
    }
};

/** Writes data to the specified table and to all dependent materialized views.
  */
class PushingToViewsBlockOutputStream : public IBlockOutputStream, WithContext
{
public:
    PushingToViewsBlockOutputStream(
        const StoragePtr & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context_,
        const ASTPtr & query_ptr_,
        bool no_destination = false);

    Block getHeader() const override;
    void write(const Block & block) override;

    void flush() override;
    void writePrefix() override;
    void writeSuffix() override;
    void onProgress(const Progress & progress) override;

private:
    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;
    BlockOutputStreamPtr output;
    ReplicatedMergeTreeSink * replicated_output = nullptr;
    Poco::Logger * log;

    ASTPtr query_ptr;
    Stopwatch main_watch;

    std::vector<ViewRuntimeData> views;
    ContextMutablePtr select_context;
    ContextMutablePtr insert_context;

    void process(const Block & block, ViewRuntimeData & view);
    void checkExceptionsInViews();
    void logQueryViews();
};

class ExecutingInnerQueryFromViewTransform final : public ExceptionKeepingTransform
{
public:
    ExecutingInnerQueryFromViewTransform(const Block & header, ViewRuntimeData & view_data)
        : ExceptionKeepingTransform(header, view_data.sample_block)
        , view(std::move(view_data))
    {
    }

    String getName() const override { return "ExecutingInnerQueryFromView"; }

protected:
    void transform(Chunk & chunk) override;
    void onFinish() override;

private:
    ViewRuntimeData & view;
};

}

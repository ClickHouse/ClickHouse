#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/QueryViewsLog.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Common/Stopwatch.h>

namespace Poco
{
class Logger;
};

namespace DB
{

class ReplicatedMergeTreeSink;

struct ViewInfo
{
    const ASTPtr query;
    StorageID table_id;
    BlockOutputStreamPtr out;
    std::exception_ptr exception;
    QueryViewsLogElement::ViewRuntimeStats runtime_stats;

    void set_exception(std::exception_ptr e)
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

private:
    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;
    BlockOutputStreamPtr output;
    ReplicatedMergeTreeSink * replicated_output = nullptr;
    Poco::Logger * log;

    ASTPtr query_ptr;
    Stopwatch main_watch;

    std::vector<ViewInfo> views;
    ContextMutablePtr select_context;
    ContextMutablePtr insert_context;

    void process(const Block & block, ViewInfo & view);
    void process_prefix(ViewInfo & view);
    void process_suffix(ViewInfo & view);
    void check_exceptions_in_views();
    void log_query_views();
};


}

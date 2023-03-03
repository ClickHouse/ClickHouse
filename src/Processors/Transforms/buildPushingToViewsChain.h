#pragma once

#include <Interpreters/QueryViewsLog.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/Chain.h>
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
    /// A query we should run over inserted block before pushing into inner storage.
    const ASTPtr query;
    /// This structure is expected by inner storage. Will convert query result to it.
    Block sample_block;
    /// Inner storage id.
    StorageID table_id;

    /// In case of exception at any step (e.g. query execution or insertion into inner table)
    /// exception is stored here (will be stored in query views log).
    std::exception_ptr exception;
    /// Info which is needed for query views log.
    std::unique_ptr<QueryViewsLogElement::ViewRuntimeStats> runtime_stats;

    void setException(std::exception_ptr e)
    {
        exception = e;
        runtime_stats->setStatus(QueryViewsLogElement::ViewStatus::EXCEPTION_WHILE_PROCESSING);
    }
};

/** Writes data to the specified table and to all dependent materialized views.
  */
Chain buildPushingToViewsChain(
    const StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const ASTPtr & query_ptr,
    /// It is true when we should not insert into table, but only to views.
    /// Used e.g. for kafka. We should try to remove it somehow.
    bool no_destination,
    /// We could specify separate thread_status for each view.
    /// Needed mainly to collect counters separately. Should be improved.
    ThreadStatus * thread_status,
    /// Counter to measure time spent separately per view. Should be improved.
    std::atomic_uint64_t * elapsed_counter_ms,
    /// LiveView executes query itself, it needs source block structure.
    const Block & live_view_header = {});

}

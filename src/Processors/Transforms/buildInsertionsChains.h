#pragma once

#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/Chain.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/IStorage.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>

namespace DB
{

Chain buildInsertionChain(
    std::function <Chain()> generate_begin

);

// /** Writes data to the specified table and to all dependent materialized views.
//   */
// Chain buildPushingToViewsChain(
//     const StoragePtr & storage,
//     const StorageMetadataPtr & metadata_snapshot,
//     ContextPtr context,
//     const ASTPtr & query_ptr,
//     size_t view_level,
//     /// It is true when we should not insert into table, but only to views.
//     /// Used e.g. for kafka. We should try to remove it somehow.
//     bool no_destination,
//     /// We could specify separate thread_status for each view.
//     /// Needed mainly to collect counters separately. Should be improved.
//     ThreadStatusesHolderPtr thread_status_holder,
//     /// Usually current_thread->getThreadGroup(), but sometimes ThreadStatus
//     /// may not have ThreadGroup (i.e. Buffer background flush), and in this
//     /// case it should be passed outside.
//     ThreadGroupPtr running_group,
//     /// Counter to measure time spent separately per view. Should be improved.
//     std::atomic_uint64_t * elapsed_counter_ms,
//     /// True if it's part of async insert flush
//     bool async_insert,
//     /// LiveView executes query itself, it needs source block structure.
//     const Block & live_view_header = {});

}

#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class AsynchronousInsertQueue;
struct BlockIO;
class ASTInsertQuery;
struct Settings;

/// Deferred async INSERT ... SELECT. Runs when the caller executes the outer pipeline, keeping
/// the operation under the normal query lifecycle so failures surface as EXCEPTION events rather
/// than errors reported before the query starts.
///
/// Pulls the SELECT pipeline (columns renamed to insert-schema names by position; types are left
/// as-is so that schema conversion happens exactly once inside the insert pipeline). The async
/// queue path is taken only when the whole result is a single block within
/// `async_insert_max_data_size`; that block is pushed to the queue (wait is unconditionally
/// forced so errors are not silenced). Any other shape (more than one block, an oversized block,
/// or an empty result) falls back to a synchronous insert that reuses the already pulled blocks,
/// so the SELECT is never run again.
///
/// When `insert_null_as_default` is on and a Nullable SELECT column feeds a non-nullable target,
/// the queue flush cannot substitute the default. `buildAsyncInsertSelectPipeline` detects this
/// and applies the full type-conversion + NULL-to-default substitution on the SELECT pipeline,
/// then forces the synchronous fallback (`needs_null_default_sync`).
class AsyncInsertSelectSource final : public ISource
{
public:
    AsyncInsertSelectSource(
        QueryPipeline select_pipeline_,
        AsynchronousInsertQueue * queue_,
        ContextMutablePtr insert_context_,
        ContextMutablePtr context_,
        ASTPtr query_ast_,
        UInt64 max_data_size_,
        UInt64 wait_timeout_ms_,
        bool insert_allow_materialized_,
        StorageID table_id_,
        bool needs_null_default_sync_);

    String getName() const override { return "AsyncInsertSelectSource"; }

protected:
    Chunk generate() override;

private:
    QueryPipeline select_pipeline;
    AsynchronousInsertQueue * queue;
    ContextMutablePtr insert_context;
    ContextMutablePtr context;
    ASTPtr query_ast;
    UInt64 max_data_size;
    UInt64 wait_timeout_ms;
    bool insert_allow_materialized;
    StorageID table_id;
    bool needs_null_default_sync;
    bool done = false;
    LoggerPtr log;
};

void buildAsyncInsertSelectPipeline(
    BlockIO & res,
    ASTInsertQuery & insert_query,
    const ASTPtr & query_ast,
    const StoragePtr & destination,
    AsynchronousInsertQueue * queue,
    ContextMutablePtr context,
    const Settings & settings,
    LoggerPtr log);

}

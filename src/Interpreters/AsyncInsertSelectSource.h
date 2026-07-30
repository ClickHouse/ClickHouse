#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InsertDependenciesBuilder.h>
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

/// Deferred async INSERT ... SELECT: runs inside the outer pipeline so failures surface as
/// EXCEPTION events instead of errors before the query starts.
///
/// The SELECT pipeline is already converted to the insert schema by
/// `InterpreterInsertQuery::convertSelectToInsertSchema`. A result that fits in one block within
/// `async_insert_max_data_size` goes to the async queue; any other shape (several blocks, an
/// oversized block, zero rows) falls back to a synchronous insert that reuses the pulled blocks,
/// so the SELECT never runs twice.
///
/// `convertSelectToInsertSchema` widens columns to `Nullable` under `insert_null_as_default`. The
/// queue flush has no defaults step to undo that, so such a widening (`needs_null_default_sync`)
/// forces the synchronous fallback, whose insert pipeline performs the substitution.
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
        bool needs_null_default_sync_,
        InsertDependenciesBuilder::ConstPtr forced_insert_dependencies_);

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
    /// Frozen before the SELECT runs; see the freeze comment in buildAsyncInsertSelectPipeline.
    InsertDependenciesBuilder::ConstPtr forced_insert_dependencies;
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

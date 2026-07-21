#pragma once

#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/StorageID.h>
#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class AsynchronousInsertQueue;
struct BlockIO;
class ASTInsertQuery;
struct Settings;

/// Deferred async INSERT ... SELECT FROM input(). Runs when the caller executes the pipeline, so it
/// stays under the normal query lifecycle and failures are logged as EXCEPTION, not before-start.
/// Pulls the SELECT, squashes up to async_insert_max_data_size, then pushes one block to the async
/// queue or, on overflow, falls back to a synchronous insert.
class AsyncInsertSelectViaInputSource final : public ISource
{
public:
    AsyncInsertSelectViaInputSource(
        QueryPipeline select_pipeline_,
        AsynchronousInsertQueue * queue_,
        ContextMutablePtr insert_context_,
        ContextMutablePtr context_,
        ASTPtr query_ast_,
        UInt64 max_data_size_,
        bool wait_for_async_insert_,
        UInt64 wait_timeout_ms_,
        bool insert_allow_materialized_,
        StorageID table_id_);

    String getName() const override { return "AsyncInsertSelectViaInput"; }

protected:
    Chunk generate() override;

private:
    QueryPipeline select_pipeline;
    AsynchronousInsertQueue * queue;
    ContextMutablePtr insert_context;
    ContextMutablePtr context;
    ASTPtr query_ast;
    UInt64 max_data_size;
    bool wait_for_async_insert;
    UInt64 wait_timeout_ms;
    bool insert_allow_materialized;
    StorageID table_id;
    bool done = false;
    LoggerPtr log;
};

void buildAsyncInsertSelectViaInputPipeline(
    BlockIO & res,
    ASTInsertQuery & insert_query,
    const ASTPtr & query_ast,
    const StoragePtr & destination,
    AsynchronousInsertQueue * queue,
    ContextMutablePtr context,
    const Settings & settings,
    LoggerPtr log);

}

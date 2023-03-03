#pragma once

#include <QueryPipeline/BlockIO.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class Chain;
class ThreadStatus;

/** Interprets the INSERT query.
  */
class InterpreterInsertQuery : public IInterpreter, WithContext
{
public:
    InterpreterInsertQuery(
        const ASTPtr & query_ptr_,
        ContextPtr context_,
        bool allow_materialized_ = false,
        bool no_squash_ = false,
        bool no_destination_ = false,
        bool async_insert_ = false);

    /** Prepare a request for execution. Return block streams
      * - the stream into which you can write data to execute the query, if INSERT;
      * - the stream from which you can read the result of the query, if SELECT and similar;
      * Or nothing if the request INSERT SELECT (self-sufficient query - does not accept the input data, does not return the result).
      */
    BlockIO execute() override;

    StorageID getDatabaseTable() const;

    Chain buildChain(
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        const Names & columns,
        ThreadStatus * thread_status = nullptr,
        std::atomic_uint64_t * elapsed_counter_ms = nullptr);

    static void extendQueryLogElemImpl(QueryLogElement & elem, ContextPtr context_);
    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context_) const override;

    StoragePtr getTable(ASTInsertQuery & query);
    Block getSampleBlock(const ASTInsertQuery & query, const StoragePtr & table, const StorageMetadataPtr & metadata_snapshot) const;

private:
    Block getSampleBlock(const Names & names, const StoragePtr & table, const StorageMetadataPtr & metadata_snapshot) const;

    ASTPtr query_ptr;
    const bool allow_materialized;
    const bool no_squash;
    const bool no_destination;
    const bool async_insert;

    Chain buildChainImpl(
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        const Block & query_sample_block,
        ThreadStatus * thread_status,
        std::atomic_uint64_t * elapsed_counter_ms);
};


}

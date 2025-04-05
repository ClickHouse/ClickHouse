#pragma once

#include <QueryPipeline/BlockIO.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class Chain;
class ReadBuffer;

/** Interprets the INSERT query.
  */
class InterpreterInsertQuery : public IInterpreter, WithContext
{
public:
    InterpreterInsertQuery(
        const ASTPtr & query_ptr_,
        ContextPtr context_,
        bool allow_materialized_,
        bool no_squash_,
        bool no_destination,
        bool async_insert_);

    /** Prepare a request for execution. Return block streams
      * - the stream into which you can write data to execute the query, if INSERT;
      * - the stream from which you can read the result of the query, if SELECT and similar;
      * Or nothing if the request INSERT SELECT (self-sufficient query - does not accept the input data, does not return the result).
      */
    BlockIO execute() override;

    StorageID getDatabaseTable() const;

    /// Return explicitly specified column names to insert.
    /// It not explicit names were specified, return nullopt.
    std::optional<Names> getInsertColumnNames() const;

    static void extendQueryLogElemImpl(QueryLogElement & elem, ContextPtr context_);

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context_) const override;

    StoragePtr getTable(ASTInsertQuery & query);

    static Block getSampleBlock(
        const ASTInsertQuery & query,
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context_,
        bool no_destination = false,
        bool allow_materialized = false);

    static Block getSampleBlock(
      const Names & names,
      const StoragePtr & table,
      const StorageMetadataPtr & metadata_snapshot,
      bool allow_virtuals,
      bool allow_materialized);

    bool supportsTransactions() const override { return true; }

    void addBuffer(std::unique_ptr<ReadBuffer> buffer);

    static bool shouldAddSquashingForStorage(const StoragePtr & table, ContextPtr context);

private:

    ASTPtr query_ptr;
    const bool allow_materialized;
    bool no_squash = false;
    bool no_destination = false;
    const bool async_insert;

    size_t max_threads = 0;
    size_t max_insert_threads = 0;

    std::vector<std::unique_ptr<ReadBuffer>> owned_buffers;

    QueryPipeline buildInsertSelectPipeline(ASTInsertQuery & query, StoragePtr table);
    QueryPipeline buildInsertPipeline(ASTInsertQuery & query, StoragePtr table);
};


}

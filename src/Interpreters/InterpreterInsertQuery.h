#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/** Interprets the INSERT query.
  */
class InterpreterInsertQuery : public IInterpreter, WithContext
{
public:
    using ReadBuffers = std::vector<std::unique_ptr<ReadBuffer>>;

    InterpreterInsertQuery(
        const ASTPtr & query_ptr_,
        ContextPtr context_,
        bool allow_materialized_ = false,
        bool no_squash_ = false,
        bool no_destination_ = false);

    InterpreterInsertQuery(
        const ASTPtr & query_ptr_,
        ReadBuffers read_buffers_,
        ContextPtr context_,
        bool allow_materialized_ = false,
        bool no_squash_ = false,
        bool no_destination_ = false);

    /** Prepare a request for execution. Return block streams
      * - the stream into which you can write data to execute the query, if INSERT;
      * - the stream from which you can read the result of the query, if SELECT and similar;
      * Or nothing if the request INSERT SELECT (self-sufficient query - does not accept the input data, does not return the result).
      */
    BlockIO execute() override;

    StorageID getDatabaseTable() const;

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context_) const override;

private:
    StoragePtr getTable(ASTInsertQuery & query);
    Block getSampleBlock(const ASTInsertQuery & query, const StoragePtr & table, const StorageMetadataPtr & metadata_snapshot) const;

    ASTPtr query_ptr;
    ReadBuffers read_buffers;
    const bool allow_materialized;
    const bool no_squash;
    const bool no_destination;
};


}

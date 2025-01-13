#pragma once

#include <QueryPipeline/BlockIO.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/ThreadStatus.h>

namespace DB
{

class Chain;
class ThreadStatus;

struct ThreadStatusesHolder;
using ThreadStatusesHolderPtr = std::shared_ptr<ThreadStatusesHolder>;

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

    Chain buildChain(
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        const Names & columns,
        ThreadStatusesHolderPtr thread_status_holder = {},
        std::atomic_uint64_t * elapsed_counter_ms = nullptr,
        bool check_access = false);

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

    bool supportsTransactions() const override { return true; }

    void addBuffer(std::unique_ptr<ReadBuffer> buffer) { owned_buffers.push_back(std::move(buffer)); }

    bool shouldAddSquashingFroStorage(const StoragePtr & table) const;

private:
    static Block getSampleBlockImpl(const Names & names, const StoragePtr & table, const StorageMetadataPtr & metadata_snapshot, bool no_destination, bool allow_materialized);

    ASTPtr query_ptr;
    const bool allow_materialized;
    bool no_squash = false;
    bool no_destination = false;
    const bool async_insert;

    std::vector<std::unique_ptr<ReadBuffer>> owned_buffers;

    std::pair<std::vector<Chain>, std::vector<Chain>> buildPreAndSinkChains(size_t presink_streams, size_t sink_streams, StoragePtr table, const StorageMetadataPtr & metadata_snapshot, const Block & query_sample_block);

    QueryPipeline buildInsertSelectPipeline(ASTInsertQuery & query, StoragePtr table);
    QueryPipeline buildInsertPipeline(ASTInsertQuery & query, StoragePtr table);

    Chain buildSink(
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        ThreadStatusesHolderPtr thread_status_holder,
        ThreadGroupPtr running_group,
        std::atomic_uint64_t * elapsed_counter_ms);

    Chain buildPreSinkChain(
        const Block & subsequent_header,
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        const Block & query_sample_block);
};


}

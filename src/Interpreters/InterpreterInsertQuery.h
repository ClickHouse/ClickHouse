#pragma once

#include <Core/HTTPHeaderColumns.h>
#include <Core/Names.h>
#include <QueryPipeline/BlockIO.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class Chain;
class ReadBuffer;

class ParallelReplicasReadingCoordinator;
using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

/** Interprets the INSERT query.
  */
class InterpreterInsertQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterInsertQuery(
        const ASTPtr & query_ptr_,
        ContextMutablePtr context_,
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

    /// Skip the target-table `INSERT` access check for this query. Used only for the internal populate of
    /// `CREATE TABLE ... AS SELECT` published via `doCreateOrReplaceTable`: the target is a random
    /// `_tmp_replace_*` name that the user neither holds nor needs `INSERT` on, and the final-name `INSERT`
    /// privilege is verified up front by the caller. The source `SELECT` access is still checked as the user.
    /// Never set this for a user-visible target table.
    void setSkipTargetInsertAccessCheck(bool skip) { skip_target_insert_access_check = skip; }

    static bool shouldAddSquashingForStorage(const StoragePtr & table, ContextPtr context);

    /// Validates http_column_* mappings against the table schema and, when the INSERT
    /// has an explicit column list, appends the mapped column names to it so that
    /// getSampleBlock includes them in the pipeline header (treating them as client-provided).
    /// Throws if any mapped column is non-insertable or conflicts with the explicit list.
    static void expandInsertQueryWithHTTPHeaderColumns(
        ASTInsertQuery & query,
        const StorageMetadataPtr & metadata_snapshot,
        const HTTPHeaderColumns & http_header_columns,
        bool allow_materialized = false);

    static void setInsertContextValues(ContextMutablePtr context_, const ASTInsertQuery & insert_query, const StoragePtr & table);

private:
    static Block getSampleBlock(
        const Names & names,
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        bool allow_virtuals,
        bool allow_materialized);

    LoggerPtr logger;
    ASTPtr query_ptr;
    const bool allow_materialized;
    bool no_squash = false;
    bool no_destination = false;
    const bool async_insert;
    bool select_query_sorted = false;
    bool skip_target_insert_access_check = false;

    size_t max_threads = 0;
    size_t max_insert_threads = 0;

    QueryPipeline buildInsertSelectPipeline(ASTInsertQuery & query, StoragePtr table);
    QueryPipeline addInsertToSelectPipeline(ASTInsertQuery & query, StoragePtr table, QueryPipelineBuilder & pipeline_builder);
    QueryPipeline buildInsertPipeline(ASTInsertQuery & query, StoragePtr table);

    std::optional<QueryPipeline> buildInsertSelectPipelineParallelReplicas(ASTInsertQuery & query, StoragePtr table);
    std::pair<QueryPipeline, ClusterProxy::LocalPlanParallelReplicasInfo>
    buildLocalInsertSelectPipelineForParallelReplicas(ASTInsertQuery & query, const StoragePtr & table, ContextPtr select_context);

    // if applicable, build pipeline for replicated MergeTree from cluster storage
    std::optional<QueryPipeline>
    distributedWriteIntoReplicatedMergeTreeOrDataLakeFromClusterStorage(const ASTInsertQuery & query, ContextPtr local_context);
};

}

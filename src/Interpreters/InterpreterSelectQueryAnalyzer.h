#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Storages/MergeTree/RequestResponse.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Planner/Planner.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class InterpreterSelectQueryAnalyzer : public IInterpreter
{
public:
    /// Initialize interpreter with query AST
    InterpreterSelectQueryAnalyzer(const ASTPtr & query_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_);

    /** Initialize interpreter with query AST and storage.
      * After query tree is built left most table expression is replaced with table node that
      * is initialized with provided storage.
      */
    InterpreterSelectQueryAnalyzer(const ASTPtr & query_,
        const ContextPtr & context_,
        const StoragePtr & storage_,
        const SelectQueryOptions & select_query_options_);

    /// Initialize interpreter with query tree
    InterpreterSelectQueryAnalyzer(const QueryTreeNodePtr & query_tree_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_);

    ContextPtr getContext() const
    {
        return context;
    }

    Block getSampleBlock();

    static Block getSampleBlock(const ASTPtr & query,
        const ContextPtr & context,
        const SelectQueryOptions & select_query_options = {});

    static Block getSampleBlock(const QueryTreeNodePtr & query_tree,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options = {});

    BlockIO execute() override;

    QueryPlan & getQueryPlan();

    QueryPlan && extractQueryPlan() &&;

    QueryPipelineBuilder buildQueryPipeline();

    void addStorageLimits(const StorageLimitsList & storage_limits);

    bool supportsTransactions() const override { return true; }

    bool ignoreLimits() const override { return select_query_options.ignore_limits; }

    bool ignoreQuota() const override { return select_query_options.ignore_quota; }

    /// Set merge tree read task callback in context and set collaborate_with_initiator in client info
    void setMergeTreeReadTaskCallbackAndClientInfo(MergeTreeReadTaskCallback && callback);

    /// Set number_of_current_replica and count_participating_replicas in client_info
    void setProperClientInfo(size_t replica_number, size_t count_participating_replicas);

    const Planner & getPlanner() const { return planner; }

private:
    ASTPtr query;
    ContextMutablePtr context;
    SelectQueryOptions select_query_options;
    QueryTreeNodePtr query_tree;
    Planner planner;
};

}

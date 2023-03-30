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

    /// Initialize interpreter with query tree
    InterpreterSelectQueryAnalyzer(const QueryTreeNodePtr & query_tree_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_);

    ContextPtr getContext() const
    {
        return context;
    }

    Block getSampleBlock();

    BlockIO execute() override;

    QueryPlan && extractQueryPlan() &&;

    QueryPipelineBuilder buildQueryPipeline();

    void addStorageLimits(const StorageLimitsList & storage_limits);

    bool supportsTransactions() const override { return true; }

    bool ignoreLimits() const override { return select_query_options.ignore_limits; }

    bool ignoreQuota() const override { return select_query_options.ignore_quota; }

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override;

    /// Set merge tree read task callback in context and set collaborate_with_initiator in client info
    void setMergeTreeReadTaskCallbackAndClientInfo(MergeTreeReadTaskCallback && callback);

    /// Set number_of_current_replica and count_participating_replicas in client_info
    void setProperClientInfo(size_t replica_number, size_t count_participating_replicas);

private:
    ASTPtr query;
    ContextMutablePtr context;
    SelectQueryOptions select_query_options;
    QueryTreeNodePtr query_tree;
    Planner planner;
};

}

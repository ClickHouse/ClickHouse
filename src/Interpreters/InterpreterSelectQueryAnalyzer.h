#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Planner/Planner.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class QueryPlan;

class InterpreterSelectQueryAnalyzer : public IInterpreter
{
public:
    /// Initialize interpreter with query AST
    InterpreterSelectQueryAnalyzer(const ASTPtr & query_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_,
        const Names & column_names = {});

    /** Initialize interpreter with query AST and storage.
      * After query tree is built left most table expression is replaced with table node that
      * is initialized with provided storage.
      */
    InterpreterSelectQueryAnalyzer(
        const ASTPtr & query_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_,
        const StoragePtr & storage_,
        const Names & column_names = {});

    /** Initialize interpreter with query tree.
      * No query tree passes are applied.
      */
    InterpreterSelectQueryAnalyzer(const QueryTreeNodePtr & query_tree_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_);

    ContextPtr getContext() const
    {
        return context;
    }

    SharedHeader getSampleBlock();
    std::pair<SharedHeader, PlannerContextPtr> getSampleBlockAndPlannerContext();

    static SharedHeader getSampleBlock(const ASTPtr & query,
        const ContextPtr & context,
        const SelectQueryOptions & select_query_options = {});

    static SharedHeader getSampleBlock(const QueryTreeNodePtr & query_tree,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options = {});

    static std::pair<SharedHeader, PlannerContextPtr> getSampleBlockAndPlannerContext(const QueryTreeNodePtr & query_tree,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options = {});

    BlockIO execute() override;

    QueryPlan & getQueryPlan();

    QueryPlan && extractQueryPlan() &&;

    QueryPipelineBuilder buildQueryPipeline();

    void addStorageLimits(const StorageLimitsList & storage_limits);

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr /*context*/) const override;

    bool supportsTransactions() const override { return true; }

    bool ignoreLimits() const override { return select_query_options.ignore_limits; }

    bool ignoreQuota() const override { return select_query_options.ignore_quota; }

    const Planner & getPlanner() const { return planner; }

    Planner & getPlanner() { return planner; }

    const QueryTreeNodePtr & getQueryTree() const { return query_tree; }

private:
    ASTPtr query;
    ContextMutablePtr context;
    SelectQueryOptions select_query_options;
    QueryTreeNodePtr query_tree;
    Planner planner;

    std::function<std::unique_ptr<QueryPlan>()> query_plan_with_parallel_replicas_builder;
};

void replaceStorageInQueryTree(QueryTreeNodePtr & query_tree, const ContextPtr & context, const StoragePtr & storage);

}

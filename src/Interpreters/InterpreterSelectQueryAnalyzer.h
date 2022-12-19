#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Analyzer/QueryTreePassManager.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context_fwd.h>

#include <Planner/Planner.h>

namespace DB
{

class InterpreterSelectQueryAnalyzer : public IInterpreter, public WithContext
{
public:
    /// Initialize interpreter with query AST
    InterpreterSelectQueryAnalyzer(const ASTPtr & query_,
        const SelectQueryOptions & select_query_options_,
        ContextPtr context_);

    /// Initialize interpreter with query tree
    InterpreterSelectQueryAnalyzer(const QueryTreeNodePtr & query_tree_,
        const SelectQueryOptions & select_query_options_,
        ContextPtr context_);

    Block getSampleBlock();

    BlockIO execute() override;

    QueryPlan && extractQueryPlan() &&;

    bool supportsTransactions() const override { return true; }

    bool ignoreLimits() const override { return select_query_options.ignore_limits; }

    bool ignoreQuota() const override { return select_query_options.ignore_quota; }

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override;

private:
    ASTPtr query;
    QueryTreeNodePtr query_tree;
    SelectQueryOptions select_query_options;
    Planner planner;
};

}

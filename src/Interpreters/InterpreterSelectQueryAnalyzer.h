#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Analyzer/QueryTreePassManager.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class InterpreterSelectQueryAnalyzer : public IInterpreter, public WithContext
{
public:
    /// Initialize interpreter with query AST
    InterpreterSelectQueryAnalyzer(
        const ASTPtr & query_,
        const SelectQueryOptions & select_query_options_,
        ContextPtr context_);

    /// Initialize interpreter with query tree after query analysis and others phases
    InterpreterSelectQueryAnalyzer(
        const QueryTreeNodePtr & query_tree_,
        const SelectQueryOptions & select_query_options_,
        ContextPtr context_);

    Block getSampleBlock();

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }

private:
    void initializeQueryPlanIfNeeded();

    ASTPtr query;
    QueryTreeNodePtr query_tree;
    QueryPlan query_plan;
    SelectQueryOptions select_query_options;
};

}

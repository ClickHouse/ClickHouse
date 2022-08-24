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

    Block getSampleBlock();

    QueryPlan && extractQueryPlan() &&;

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }

private:
    ASTPtr query;
    QueryTreeNodePtr query_tree;
    SelectQueryOptions select_query_options;
    Planner planner;
};

}

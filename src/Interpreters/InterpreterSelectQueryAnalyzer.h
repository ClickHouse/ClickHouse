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
    InterpreterSelectQueryAnalyzer(
        const ASTPtr & query_ptr_,
        const SelectQueryOptions & select_query_options_,
        ContextPtr context);

    Block getSampleBlock();

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }

private:
    void initializeQueryPlanIfNeeded();

    ASTPtr query_ptr;
    QueryPlan query_plan;
    SelectQueryOptions select_query_options;
    QueryTreePassManager query_tree_pass_manager;
};

}

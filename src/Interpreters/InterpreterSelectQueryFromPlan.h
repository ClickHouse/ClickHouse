#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

/// Executes a materialized cached SELECT plan without constructing the analyzer.
class InterpreterSelectQueryFromPlan : public IInterpreter
{
public:
    InterpreterSelectQueryFromPlan(
        QueryPlan && query_plan_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_);

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }
    bool ignoreLimits() const override { return select_query_options.ignore_limits; }
    bool ignoreQuota() const override { return select_query_options.ignore_quota; }

private:
    QueryPlan query_plan;
    ContextMutablePtr context;
    SelectQueryOptions select_query_options;
};

}

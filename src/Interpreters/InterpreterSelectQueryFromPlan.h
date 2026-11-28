#pragma once

#include <Core/ResultColumnOrigin.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

/// Executes a pre-built SELECT plan (e.g. a materialized query plan cache entry, or a
/// logical plan resolved against current storages) without constructing the analyzer.
class InterpreterSelectQueryFromPlan : public IInterpreter
{
public:
    InterpreterSelectQueryFromPlan(
        QueryPlan && query_plan_,
        const ContextPtr & context_,
        const SelectQueryOptions & select_query_options_,
        ResultColumnOrigins result_column_origins_ = {});

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }
    bool ignoreLimits() const override { return select_query_options.ignore_limits; }
    bool ignoreQuota() const override { return select_query_options.ignore_quota; }

    /// Where the result columns come from, as collected by the analyzer that built the plan.
    const ResultColumnOrigins & getResultColumnOrigins() const { return result_column_origins; }

private:
    QueryPlan query_plan;
    ContextMutablePtr context;
    SelectQueryOptions select_query_options;
    ResultColumnOrigins result_column_origins;
};

}

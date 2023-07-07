#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <QueryCoordination/PlanFragment.h>

namespace DB
{

class InterpreterSelectQuery;
class QueryPlan;

/** Interprets one or multiple SELECT queries inside UNION/UNION ALL/UNION DISTINCT chain.
  */
class InterpreterSelectWithUnionQueryFragments : public IInterpreterUnionOrSelectQuery
{
public:
    using IInterpreterUnionOrSelectQuery::getSampleBlock;

    InterpreterSelectWithUnionQueryFragments(
        const ASTPtr & query_ptr_,
        ContextPtr context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {});

    InterpreterSelectWithUnionQueryFragments(
        const ASTPtr & query_ptr_,
        ContextMutablePtr context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {});

    ~InterpreterSelectWithUnionQueryFragments() override;

    /// Builds QueryPlan for current query.
    void buildQueryPlan(QueryPlan & query_plan) override;

    void buildFragments();

    BlockIO execute() override;

    bool ignoreLimits() const override { return options.ignore_limits; }
    bool ignoreQuota() const override { return options.ignore_quota; }

    static Block getSampleBlock(
        const ASTPtr & query_ptr_,
        ContextPtr context_,
        bool is_subquery = false,
        bool is_create_parameterized_view = false);

    void ignoreWithTotals() override;

    bool supportsTransactions() const override { return true; }

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context) const override;

private:
    std::vector<std::unique_ptr<IInterpreterUnionOrSelectQuery>> nested_interpreters;

    PlanFragmentPtrs fragments;

    static Block getCommonHeaderForUnion(const Blocks & headers);

    Block getCurrentChildResultHeader(const ASTPtr & ast_ptr_, const Names & required_result_column_names);

    std::unique_ptr<IInterpreterUnionOrSelectQuery>
    buildCurrentChildInterpreter(const ASTPtr & ast_ptr_, const Names & current_required_result_column_names);
};

}

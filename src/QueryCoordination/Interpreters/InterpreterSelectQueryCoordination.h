#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Optimizer/SubQueryPlan.h>

namespace DB
{

class InterpreterSelectQueryCoordination : public IInterpreter
{
public:
    InterpreterSelectQueryCoordination(const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions &);

    InterpreterSelectQueryCoordination(const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions &);

    BlockIO execute() override;

    void explain(WriteBufferFromOwnString & buf, const QueryPlan::ExplainPlanOptions & options_, bool json, bool optimize_);
    void explainFragment(WriteBufferFromOwnString & buf, const Fragment::ExplainFragmentOptions & options_);

    /// Disable use_index_for_in_with_subqueries.
    void setIncompatibleSettings();

    bool ignoreQuota() const override { return false; }
    bool ignoreLimits() const override { return false; }

    void extendQueryLogElemImpl(QueryLogElement &, const ASTPtr &, ContextPtr) const override { }

    /// Returns true if transactions maybe supported for this type of query.
    /// If Interpreter returns true, than it is responsible to check that specific query with specific Storage is supported.
    bool supportsTransactions() const override { return false; }

    ContextPtr getContext() const { return context; }

private:
    void buildQueryPlanIfNeeded();

    /// Optimize query plan, if query_coordination_enabled is true build sub_plan.
    void optimize();

    /// Build distributed query plan.
    void buildFragments();

    ASTPtr query_ptr;
    ContextMutablePtr context;
    SelectQueryOptions options;

    /// Query coordination is enabled if
    ///     1. there is no table function
    ///     2. there is no local table
    ///     3. allow_experimental_query_coordination = 1
    bool query_coordination_enabled;

    /// Query plan
    QueryPlan plan;

    /// Distributed query plan, enabled only if query_coordination_enabled is true.
    FragmentPtrs fragments;
};

}

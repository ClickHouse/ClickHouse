#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Optimizer/StepTree.h>

namespace DB
{

class InterpreterSelectQueryCoordination : public IInterpreter
{
public:
    InterpreterSelectQueryCoordination(
            const ASTPtr & query_ptr_,
            ContextPtr context_,
            const SelectQueryOptions &);

    InterpreterSelectQueryCoordination(
        const ASTPtr & query_ptr_,
        ContextMutablePtr context_,
        const SelectQueryOptions &);

    BlockIO execute() override;

    void explain(WriteBufferFromOwnString & buf, const QueryPlan::ExplainPlanOptions & options_, bool json, bool optimize_);
    void explainFragment(WriteBufferFromOwnString & buf, const Fragment::ExplainFragmentOptions & options_);

    bool checkCompatibleSettings() const;

    bool ignoreQuota() const override { return false; }
    bool ignoreLimits() const override { return false; }

    void extendQueryLogElemImpl(QueryLogElement &, const ASTPtr &, ContextPtr) const override {}

    /// Returns true if transactions maybe supported for this type of query.
    /// If Interpreter returns true, than it is responsible to check that specific query with specific Storage is supported.
    bool supportsTransactions() const override { return false; }

    ContextPtr getContext() const
    {
        return context;
    }

private:
    void buildQueryPlanIfNeeded();

    void optimize();
    void buildFragments();

    ASTPtr query_ptr;
    ContextMutablePtr context;
    SelectQueryOptions options;
    bool query_coordination_enabled;

    QueryPlan plan;
    StepTree step_tree;
    FragmentPtrs fragments;
};

}

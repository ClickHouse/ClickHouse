#pragma once

#include <QueryCoordination/PlanNode.h>
#include <QueryCoordination/Fragments/PlanFragment.h>
#include <Interpreters/Context.h>

namespace DB
{

class StepMatcherBase
{
public:
    struct Data
    {
        ContextMutablePtr context;
        PlanFragmentPtrs all_fragments;
    };

    virtual PlanFragmentPtr doVisit(PlanNode & node, PlanFragmentPtrs child_res, Data & data) = 0;

    virtual ~StepMatcherBase() = default;
};

using StepMatcherPtr = std::shared_ptr<StepMatcherBase>;

}

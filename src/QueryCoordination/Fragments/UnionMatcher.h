#pragma once

#include <QueryCoordination/Fragments/StepMatcherBase.h>

namespace DB
{

class UnionMatcher : public StepMatcherBase
{
    PlanFragmentPtr doVisit(PlanNode & node, PlanFragmentPtrs child_res, Data & data) override
    {
        DataPartition partition{.type = PartitionType::UNPARTITIONED};
        PlanFragmentPtr parent_fragment = std::make_shared<PlanFragment>(data.context->getFragmentID(), partition, data.context);
        parent_fragment->unitePlanFragments(node.step, child_res);

        data.all_fragments.push_back(parent_fragment);
        return parent_fragment;
    }
};

}

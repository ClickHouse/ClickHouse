#pragma once

#include <QueryCoordination/Fragments/StepMatcherBase.h>

namespace DB
{

class CreatingSetsMatcher : public StepMatcherBase
{
    PlanFragmentPtr doVisit(PlanNode & node, PlanFragmentPtrs /*child_res*/, Data & data) override
    {
        DataPartition partition;
        partition.type = PartitionType::RANDOM;

        auto fragment = std::make_shared<PlanFragment>(data.context->getFragmentID(), partition, data.context);
        fragment->addStep(std::move(node.step));
        fragment->setCluster(data.context->getCluster("test_two_shards"));

        return fragment;
    }
};

}

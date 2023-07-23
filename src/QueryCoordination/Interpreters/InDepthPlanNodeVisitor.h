#pragma once

#include <QueryCoordination/PlanNode.h>
#include <QueryCoordination/Fragments/PlanFragment.h>
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{


//template <typename Result, typename Results = std::vector<Result>> Result doVisit(StepType type, PlanNode & node, Results child_res);

template <typename Result, typename T = PlanNode>
class Matcher
{
public:
    using Results = std::vector<Result>;
    virtual Result doVisit(T & node, Results child_res);

    virtual ~Matcher() = default;
};



class StepMatcher : public Matcher<PlanFragmentPtr>
{
};

class SourceMatcher : public StepMatcher
{
    PlanFragmentPtr doVisit(PlanNode & /*node*/, PlanFragmentPtrs /*child_res*/) override
    {
//            DataPartition partition;
            //    partition.type = PartitionType::RANDOM;
            //
            //    auto fragment = std::make_shared<PlanFragment>(context->getFragmentID(), partition, context);
            //    fragment->addStep(std::move(step));
            //    //    fragment->setFragmentInPlanTree(fragment->getRootNode());
            //    fragment->setCluster(context->getCluster("test_two_shards"));

            return {};
            //    return fragment;
    }
};


template <typename Result, typename T = PlanNode>
class InDepthPlanNodeVisitor
{
public:
    using Results = std::vector<Result>;

    Result visit(T & node)
    {
        Results child_res;
        for (T * child : node.children)
        {
            child_res.emplace_back(visit(*child));
        }

        /// do some thing
        Result res = createMatcher(node.step)->doVisit(node, child_res);

        return res;
    }
};

using PlanFragmentVisitor = InDepthPlanNodeVisitor<PlanFragmentPtr>;


//template <> PlanFragmentPtr doVisit<PlanFragmentPtr>(PlanNode & /*step*/, PlanFragmentPtrs /*child_res*/)
//{
////    DataPartition partition;
////    partition.type = PartitionType::RANDOM;
////
////    auto fragment = std::make_shared<PlanFragment>(context->getFragmentID(), partition, context);
////    fragment->addStep(std::move(step));
////    //    fragment->setFragmentInPlanTree(fragment->getRootNode());
////    fragment->setCluster(context->getCluster("test_two_shards"));
//
//    return {};
////    return fragment;
//}
//
//
//template <> PlanFragmentPtr doVisit<StepType::Agg, PlanFragmentPtr>(PlanNode & /*step*/, PlanFragmentPtrs /*child_res*/)
//{
//    //    DataPartition partition;
//    //    partition.type = PartitionType::RANDOM;
//    //
//    //    auto fragment = std::make_shared<PlanFragment>(context->getFragmentID(), partition, context);
//    //    fragment->addStep(std::move(step));
//    //    //    fragment->setFragmentInPlanTree(fragment->getRootNode());
//    //    fragment->setCluster(context->getCluster("test_two_shards"));
//
//    return {};
//    //    return fragment;
//}

}

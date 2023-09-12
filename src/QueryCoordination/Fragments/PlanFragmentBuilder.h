//#pragma once
//
//#include <Processors/QueryPlan/QueryPlan.h>
//#include <QueryCoordination/PlanNode.h>
//#include <QueryCoordination/DataPartition.h>
//
//namespace DB
//{
//
//using Node = QueryPlan::Node;
//
//class PlanFragmentBuilder
//{
//public:
//    PlanFragmentBuilder(StorageLimitsList storage_limits_, ContextMutablePtr context_, QueryPlan & query_plan_)
//        : storage_limits(storage_limits_), context(context_), query_plan(std::move(query_plan_))
//    {
//    }
//
//    PlanFragmentPtrs build();
//
//private:
//    PlanFragmentPtr createPlanFragments(const QueryPlan & single_plan, Node & root_node);
//    PlanFragmentPtr createOrderByFragment(QueryPlanStepPtr step, PlanFragmentPtr child_fragment);
//    PlanFragmentPtr createScanFragment(QueryPlanStepPtr step);
//    PlanFragmentPtr createAggregationFragment(QueryPlanStepPtr step, PlanFragmentPtr child_fragment);
//    PlanFragmentPtr createParentFragment(PlanFragmentPtr child_fragment, const DataPartition & partition);
//    PlanFragmentPtr createJoinFragment(QueryPlanStepPtr step, PlanFragmentPtr left_child_fragment, PlanFragmentPtr right_child_fragment);
//    PlanFragmentPtr createCreatingSetsFragment(Node & root_node, PlanFragmentPtrs child_fragments);
//    PlanFragmentPtr createUnpartitionedFragment(QueryPlanStepPtr step, PlanFragmentPtr child_fragment);
//    PlanFragmentPtr createUnionFragment(QueryPlanStepPtr step, PlanFragmentPtrs child_fragments);
//
//    void pushDownLimitRelated(QueryPlanStepPtr step, PlanFragmentPtr child_fragment);
//
//    StorageLimitsList storage_limits; /// TODO exchange limit
//
//    ContextMutablePtr context;
//
//    QueryPlan query_plan;
//
//    PlanFragmentPtrs all_fragments;
//};
//
//}

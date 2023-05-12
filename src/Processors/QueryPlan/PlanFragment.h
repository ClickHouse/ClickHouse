#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class QueryPlan;

class PlanFragment
{
public:
    using Node = QueryPlan::Node;

    PlanFragment(/*UInt32 id, */Node & root) : plan_root(std::move(root)) { }

private:

    // id for this plan fragment
//    UInt32 fragment_id;
    // nereids planner and original planner generate fragments in different order.
    // This makes nereids fragment id different from that of original planner, and
    // hence different from that in profile.
    // in original planner, fragmentSequenceNum is fragmentId, and in nereids planner,
    // fragmentSequenceNum is the id displayed in profile

//    Int32 fragmentSequenceNum;
    // private PlanId planId_;
    // private CohortId cohortId_;

    // root of plan tree executed by this fragment
    Node plan_root;

    // exchange node to which this fragment sends its output
    Node dest_node;

    // if null, outputs the entire row produced by planRoot
    // ArrayList<Expr> outputExprs;

    // created in finalize() or set in setSink()
//    DataSink sink;
};

using PlanFragmentPtr = std::unique_ptr<PlanFragment>;
using PlanFragmentPtrs = std::vector<PlanFragmentPtr>;

}

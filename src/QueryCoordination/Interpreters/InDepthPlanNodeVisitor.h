#pragma once

#include <QueryCoordination/PlanNode.h>
#include <QueryCoordination/Fragments/PlanFragment.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryCoordination/Fragments/createMatcher.h>

namespace DB
{

template <typename Result, typename Matcher, typename T = PlanNode>
class InDepthPlanNodeVisitor
{
public:
    using Results = std::vector<Result>;

    using MatcherPtr = std::shared_ptr<Matcher>;

    using Data = typename Matcher::Data;

    explicit InDepthPlanNodeVisitor(Data & data_) : data(data_) { }

    Result visit(T & node)
    {
        Results child_res;
        for (T * child : node.children)
        {
            child_res.emplace_back(visit(*child));
        }

        /// do some thing
        MatcherPtr matcher = createMatcher<MatcherPtr, T>(node);
        Result res = matcher->doVisit(node, child_res, data);

        return res;
    }

private:
    Data & data;
};

}

#include <QueryCoordination/Fragments/createMatcher.h>

namespace DB
{

template <> StepMatcherPtr createMatcher<StepMatcherPtr, PlanNode>(PlanNode & node)
{
    if (dynamic_cast<ISourceStep *>(node.step.get()))
    {
        return std::make_shared<SourceMatcher>();
    }

    return {};
}

}


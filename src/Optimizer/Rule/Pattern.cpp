#include <Optimizer/Rule/Pattern.h>

namespace DB
{

Pattern::Pattern(StepType step_type_) : step_type(step_type_)
{
}

Pattern & Pattern::addChildren(const std::vector<Pattern> & children_)
{
    children.insert(children.end(), children_.begin(), children_.end());
    return *this;
}

StepType Pattern::getStepType() const
{
    return step_type;
}

void Pattern::setStepType(StepType step_type_)
{
    step_type = step_type_;
}

const std::vector<Pattern> & Pattern::getChildren() const
{
    return children;
}

}

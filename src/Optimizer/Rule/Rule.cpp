#include <Optimizer/Rule/Rule.h>

namespace DB
{

size_t Rule::getRuleId() const
{
    return id;
}

const Pattern & Rule::getPattern() const
{
    return pattern;
}

}

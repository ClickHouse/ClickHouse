#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

const Pattern & Rule::getPattern() const
{
    return pattern;
}

}

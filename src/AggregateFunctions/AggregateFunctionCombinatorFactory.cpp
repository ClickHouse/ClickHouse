#include <Common/StringUtils/StringUtils.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void AggregateFunctionCombinatorFactory::registerCombinator(const AggregateFunctionCombinatorPtr & value)
{
    if (!dict.emplace(value->getName(), value).second)
        throw Exception("AggregateFunctionCombinatorFactory: the name '" + value->getName() + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

AggregateFunctionCombinatorPtr AggregateFunctionCombinatorFactory::tryFindSuffix(const std::string & name) const
{
    /// O(N) is ok for just a few combinators.
    for (const auto & suffix_value : dict)
        if (endsWith(name, suffix_value.first))
            return suffix_value.second;
    return {};
}

AggregateFunctionCombinatorFactory & AggregateFunctionCombinatorFactory::instance()
{
    static AggregateFunctionCombinatorFactory ret;
    return ret;
}

}

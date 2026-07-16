#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>

#include <Common/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void AggregateFunctionCombinatorFactory::registerCombinator(const AggregateFunctionCombinatorPtr & value)
{
    CombinatorPair pair{
        .name = value->getName(),
        .combinator_ptr = value,
    };

    /// lower_bound() cannot be used since sort order of the dict is by length of the combinator
    /// but there are just a few combiners, so not a problem.
    if (std::find(dict.begin(), dict.end(), pair) != dict.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionCombinatorFactory: the name '{}' is not unique",
            value->getName());
    dict.emplace(std::lower_bound(dict.begin(), dict.end(), pair), pair);
}

AggregateFunctionCombinatorPtr AggregateFunctionCombinatorFactory::tryFindSuffix(const std::string & name) const
{
    /// O(N) is ok for just a few combinators.
    for (const auto & suffix_value : dict)
        if (endsWith(name, suffix_value.name))
            return suffix_value.combinator_ptr;
    return {};
}

AggregateFunctionCombinatorFactory & AggregateFunctionCombinatorFactory::instance()
{
    static AggregateFunctionCombinatorFactory ret;
    return ret;
}

}

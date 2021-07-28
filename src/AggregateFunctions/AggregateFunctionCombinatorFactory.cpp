#include <Common/StringUtils/StringUtils.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void AggregateFunctionCombinatorFactory::registerCombinator(const AggregateFunctionCombinatorPtr & value, IDocumentationPtr documentation)
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
    docs.emplace(pair.name, std::move(documentation));
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

std::string AggregateFunctionCombinatorFactory::getDocumentation(const std::string & name) const
{
    auto it = docs.find(name);
    if (docs.end() != it)
        return it->second == nullptr ? "Not found" : it->second->getDocumentation();

    return "Not found anywhere";
}
}

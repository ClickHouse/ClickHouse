#pragma once

#include <AggregateFunctions/IAggregateFunctionCombinator.h>


#include <string>
#include <unordered_map>


namespace DB
{

/** Create aggregate function combinator by matching suffix in aggregate function name.
  */
class AggregateFunctionCombinatorFactory final: private boost::noncopyable
{
private:
    using Dict = std::unordered_map<std::string, AggregateFunctionCombinatorPtr>;
    Dict dict;

public:

    static AggregateFunctionCombinatorFactory & instance();

    /// Not thread safe. You must register before using tryGet.
    void registerCombinator(const AggregateFunctionCombinatorPtr & value);

    /// Example: if the name is 'avgIf', it will return combinator -If.
    AggregateFunctionCombinatorPtr tryFindSuffix(const std::string & name) const;

    const Dict & getAllAggregateFunctionCombinators() const
    {
        return dict;
    }
};

}

#pragma once

#include <AggregateFunctions/Combinators/IAggregateFunctionCombinator.h>

#include <string>


namespace DB
{
struct Settings;

/** Create aggregate function combinator by matching suffix in aggregate function name.
  */
class AggregateFunctionCombinatorFactory final: private boost::noncopyable
{
private:
    struct CombinatorPair
    {
        std::string name;
        AggregateFunctionCombinatorPtr combinator_ptr;

        bool operator==(const CombinatorPair & rhs) const { return name == rhs.name; }
        /// Sort by the length of the combinator name for proper tryFindSuffix()
        /// for combiners with common prefix (i.e. "State" and "SimpleState").
        bool operator<(const CombinatorPair & rhs) const { return name.length() > rhs.name.length(); }
    };
    using Dict = std::vector<CombinatorPair>;
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

#pragma once

#include <AggregateFunctions/IAggregateFunctionCombinator.h>


#include <string>
#include <unordered_map>
#include <Documentation/SimpleDocumentation.h>
#include "Documentation/IDocumentation.h"

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

    using CombinatorsDocs = std::unordered_map<std::string, IDocumentationPtr>;
    CombinatorsDocs docs;

public:

    static AggregateFunctionCombinatorFactory & instance();

    /// Not thread safe. You must register before using tryGet.
    void registerCombinator(const AggregateFunctionCombinatorPtr & value, IDocumentationPtr documentation=nullptr);

    /// Example: if the name is 'avgIf', it will return combinator -If.
    AggregateFunctionCombinatorPtr tryFindSuffix(const std::string & name) const;

    const Dict & getAllAggregateFunctionCombinators() const
    {
        return dict;
    }

    /// If there will be no documentation returns "Not found"
    std::string getDocumentation(const std::string & name) const;
};

}

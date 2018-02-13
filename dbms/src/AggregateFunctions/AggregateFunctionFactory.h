#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <ext/singleton.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{

class Context;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;


/** Creates an aggregate function by name.
  */
class AggregateFunctionFactory final : public ext::singleton<AggregateFunctionFactory>
{
    friend class StorageSystemFunctions;

public:
    /** Creator have arguments: name of aggregate function, types of arguments, values of parameters.
      * Parameters are for "parametric" aggregate functions.
      * For example, in quantileWeighted(0.9)(x, weight), 0.9 is "parameter" and x, weight are "arguments".
      */
    using Creator = std::function<AggregateFunctionPtr(const String &, const DataTypes &, const Array &)>;

    /// For compatibility with SQL, it's possible to specify that certain aggregate function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const String & name,
        Creator creator,
        CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Throws an exception if not found.
    AggregateFunctionPtr get(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters = {},
        int recursion_level = 0) const;

    /// Returns nullptr if not found.
    AggregateFunctionPtr tryGet(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters = {}) const;

    bool isAggregateFunctionName(const String & name, int recursion_level = 0) const;

private:
    AggregateFunctionPtr getImpl(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        int recursion_level) const;

private:
    using AggregateFunctions = std::unordered_map<String, Creator>;

    AggregateFunctions aggregate_functions;

    /// Case insensitive aggregate functions will be additionally added here with lowercased name.
    AggregateFunctions case_insensitive_aggregate_functions;
};

}

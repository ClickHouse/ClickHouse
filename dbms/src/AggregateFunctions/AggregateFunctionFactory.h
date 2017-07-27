#pragma once

#include <unordered_map>
#include <AggregateFunctions/IAggregateFunction.h>
#include <ext/singleton.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<IDataType>;
using DataTypes = std::vector<DataTypePtr>;


/** Creates an aggregate function by name.
  */
class AggregateFunctionFactory final : public ext::singleton<AggregateFunctionFactory>
{
    friend class StorageSystemFunctions;

private:
    /// No std::function, for smaller object size and less indirection.
    using Creator = AggregateFunctionPtr(*)(const String & name, const DataTypes & argument_types, const Array & parameters);
    using AggregateFunctions = std::unordered_map<String, Creator>;

public:

    AggregateFunctionPtr get(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters = {},
        int recursion_level = 0) const;

    AggregateFunctionPtr tryGet(const String & name, const DataTypes & argument_types, const Array & parameters = {}) const;

    bool isAggregateFunctionName(const String & name, int recursion_level = 0) const;

    /// For compatibility with SQL, it's possible to specify that certain aggregate function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register an aggregate function by its name.
    void registerFunction(const String & name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

private:

    AggregateFunctionPtr getImpl(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        int recursion_level) const;

private:
    AggregateFunctions aggregate_functions;

    /// Case insensitive aggregate functions will be additionally added here with lowercased name.
    AggregateFunctions case_insensitive_aggregate_functions;
};

}

#pragma once

#include <unordered_map>
#include <AggregateFunctions/IAggregateFunction.h>
#include <common/singleton.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<IDataType>;
using DataTypes = std::vector<DataTypePtr>;


/** Creates an aggregate function by name.
  */
class AggregateFunctionFactory final : public Singleton<AggregateFunctionFactory>
{
    friend class StorageSystemFunctions;

private:
    /// No std::function, for smaller object size and less indirection.
    using Creator = AggregateFunctionPtr(*)(const String & name, const DataTypes & argument_types);
    using AggregateFunctions = std::unordered_map<String, Creator>;

public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types, int recursion_level = 0) const;
    AggregateFunctionPtr tryGet(const String & name, const DataTypes & argument_types) const;
    bool isAggregateFunctionName(const String & name, int recursion_level = 0) const;

    /// For compatibility with SQL, it's possible to specify that certain aggregate function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register an aggregate function by its name.
    void registerFunction(const String & name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    AggregateFunctionFactory(const AggregateFunctionFactory &) = delete;
    AggregateFunctionFactory & operator=(const AggregateFunctionFactory &) = delete;

private:
    AggregateFunctionPtr getImpl(const String & name, const DataTypes & argument_types, int recursion_level) const;

private:
    AggregateFunctions aggregate_functions;

    /// Case insensitive aggregate functions will be additionally added here with lowercased name.
    AggregateFunctions case_insensitive_aggregate_functions;
};

}

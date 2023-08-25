#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/IFactoryWithAliases.h>
#include <Parsers/ASTFunction.h>


#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>


namespace DB
{
struct Settings;

class Context;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

/**
 * The invoker has arguments: name of aggregate function, types of arguments, values of parameters.
 * Parameters are for "parametric" aggregate functions.
 * For example, in quantileWeighted(0.9)(x, weight), 0.9 is "parameter" and x, weight are "arguments".
 */
using AggregateFunctionCreator = std::function<AggregateFunctionPtr(const String &, const DataTypes &, const Array &, const Settings *)>;

struct AggregateFunctionWithProperties
{
    AggregateFunctionCreator creator;
    AggregateFunctionProperties properties;

    AggregateFunctionWithProperties() = default;
    AggregateFunctionWithProperties(const AggregateFunctionWithProperties &) = default;
    AggregateFunctionWithProperties & operator = (const AggregateFunctionWithProperties &) = default;

    template <typename Creator>
    requires (!std::is_same_v<Creator, AggregateFunctionWithProperties>)
    AggregateFunctionWithProperties(Creator creator_, AggregateFunctionProperties properties_ = {}) /// NOLINT
        : creator(std::forward<Creator>(creator_)), properties(std::move(properties_))
    {
    }
};


/** Creates an aggregate function by name.
  */
class AggregateFunctionFactory final : private boost::noncopyable, public IFactoryWithAliases<AggregateFunctionWithProperties>
{
public:
    static AggregateFunctionFactory & instance();

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const String & name,
        Value creator,
        CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Throws an exception if not found.
    AggregateFunctionPtr
    get(const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties) const;

    /// Returns nullptr if not found.
    AggregateFunctionPtr tryGet(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties) const;

    /// Get properties if the aggregate function exists.
    std::optional<AggregateFunctionProperties> tryGetProperties(const String & name) const;

    bool isAggregateFunctionName(const String & name) const;

private:
    AggregateFunctionPtr getImpl(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties,
        bool has_null_arguments) const;

    std::optional<AggregateFunctionProperties> tryGetPropertiesImpl(const String & name) const;

    using AggregateFunctions = std::unordered_map<String, Value>;

    AggregateFunctions aggregate_functions;

    /// Case insensitive aggregate functions will be additionally added here with lowercased name.
    AggregateFunctions case_insensitive_aggregate_functions;

    const AggregateFunctions & getMap() const override { return aggregate_functions; }

    const AggregateFunctions & getCaseInsensitiveMap() const override { return case_insensitive_aggregate_functions; }

    String getFactoryName() const override { return "AggregateFunctionFactory"; }

};

struct AggregateUtils
{
    static bool isAggregateFunction(const ASTFunction & node)
    {
        return AggregateFunctionFactory::instance().isAggregateFunctionName(node.name);
    }
};

}

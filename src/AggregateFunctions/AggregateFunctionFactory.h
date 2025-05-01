#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/NullsAction.h>
#include <Common/IFactoryWithAliases.h>

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
        Case case_sensitiveness = Case::Sensitive);

    /// Register how to transform from one aggregate function to other based on NullsAction
    /// Registers them both ways:
    /// SOURCE + RESPECT NULLS will be transformed to TARGET
    /// TARGET + IGNORE NULLS will be transformed to SOURCE
    void registerNullsActionTransformation(const String & source_ignores_nulls, const String & target_respect_nulls);

    /// Throws an exception if not found.
    AggregateFunctionPtr
    get(const String & name,
        NullsAction action,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties) const;

    /// Get properties if the aggregate function exists.
    std::optional<AggregateFunctionProperties> tryGetProperties(String name, NullsAction action) const;

    bool isAggregateFunctionName(const String & name) const;

private:
    AggregateFunctionPtr getImpl(
        const String & name,
        NullsAction action,
        const DataTypes & argument_types,
        const Array & parameters,
        AggregateFunctionProperties & out_properties,
        bool has_null_arguments) const;

    using AggregateFunctions = std::unordered_map<String, Value>;
    using ActionMap = std::unordered_map<String, String>;

    AggregateFunctions aggregate_functions;
    /// Mapping from functions with `RESPECT NULLS` modifier to actual aggregate function names
    /// Example: `any(x) RESPECT NULLS` should be executed as function `any_respect_nulls`
    ActionMap respect_nulls;
    /// Same as above for `IGNORE NULLS` modifier
    ActionMap ignore_nulls;
    std::optional<AggregateFunctionWithProperties> getAssociatedFunctionByNullsAction(const String & name, NullsAction action) const;

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

const String & getAggregateFunctionCanonicalNameIfAny(const String & name);

}

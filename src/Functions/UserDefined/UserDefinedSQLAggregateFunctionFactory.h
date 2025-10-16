#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/NullsAction.h>
#include <Common/IFactoryWithAliases.h>
#include <Interpreters/Context.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>

namespace DB {

class UserDefinedSQLAggregateFunctionFactory final : private boost::noncopyable, public IFactoryWithAliases<AggregateFunctionWithProperties>
{
public:
    static UserDefinedSQLAggregateFunctionFactory & instance();

    /// Register function for function_name in factory for specified create_function_query.
    bool registerFunction(const ContextMutablePtr &, const String &, ASTPtr, bool, bool) {
        return true;
    }

    /// Unregister function for function_name.
    bool unregisterFunction(const ContextMutablePtr &, const String &, bool) { return true;}

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

    String getFactoryName() const override { return "UserDefinedSQLAggregateFunctionFactory"; }

};


};


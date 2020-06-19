#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>

#include <Poco/String.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int LOGICAL_ERROR;
}


void AggregateFunctionFactory::registerFunction(const String & name, Value creator_with_properties, CaseSensitiveness case_sensitiveness)
{
    if (creator_with_properties.creator == nullptr)
        throw Exception("AggregateFunctionFactory: the aggregate function " + name + " has been provided "
            " a null constructor", ErrorCodes::LOGICAL_ERROR);

    if (!aggregate_functions.emplace(name, creator_with_properties).second)
        throw Exception("AggregateFunctionFactory: the aggregate function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_aggregate_functions.emplace(Poco::toLower(name), creator_with_properties).second)
        throw Exception("AggregateFunctionFactory: the case insensitive aggregate function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

static DataTypes convertLowCardinalityTypesToNested(const DataTypes & types)
{
    DataTypes res_types;
    res_types.reserve(types.size());
    for (const auto & type : types)
        res_types.emplace_back(recursiveRemoveLowCardinality(type));

    return res_types;
}

AggregateFunctionPtr AggregateFunctionFactory::get(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    int recursion_level) const
{
    auto type_without_low_cardinality = convertLowCardinalityTypesToNested(argument_types);

    /// If one of the types is Nullable, we apply aggregate function combinator "Null".

    if (std::any_of(type_without_low_cardinality.begin(), type_without_low_cardinality.end(),
        [](const auto & type) { return type->isNullable(); }))
    {
        AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix("Null");
        if (!combinator)
            throw Exception("Logical error: cannot find aggregate function combinator to apply a function to Nullable arguments.",
                ErrorCodes::LOGICAL_ERROR);

        DataTypes nested_types = combinator->transformArguments(type_without_low_cardinality);
        Array nested_parameters = combinator->transformParameters(parameters);

        bool has_null_arguments = std::any_of(type_without_low_cardinality.begin(), type_without_low_cardinality.end(),
            [](const auto & type) { return type->onlyNull(); });

        AggregateFunctionPtr nested_function = getImpl(
            name, nested_types, nested_parameters, out_properties, has_null_arguments, recursion_level);
        return combinator->transformAggregateFunction(nested_function, out_properties, type_without_low_cardinality, parameters);
    }

    auto res = getImpl(name, type_without_low_cardinality, parameters, out_properties, false, recursion_level);
    if (!res)
        throw Exception("Logical error: AggregateFunctionFactory returned nullptr", ErrorCodes::LOGICAL_ERROR);
    return res;
}


AggregateFunctionPtr AggregateFunctionFactory::getImpl(
    const String & name_param,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    bool has_null_arguments,
    int recursion_level) const
{
    String name = getAliasToOrName(name_param);
    Value found;

    /// Find by exact match.
    if (auto it = aggregate_functions.find(name); it != aggregate_functions.end())
    {
        found = it->second;
    }
    /// Find by case-insensitive name.
    /// Combinators cannot apply for case insensitive (SQL-style) aggregate function names. Only for native names.
    else if (recursion_level == 0)
    {
        if (auto jt = case_insensitive_aggregate_functions.find(Poco::toLower(name)); jt != case_insensitive_aggregate_functions.end())
            found = jt->second;
    }

    if (found.creator)
    {
        out_properties = found.properties;

        /// The case when aggregate function should return NULL on NULL arguments. This case is handled in "get" method.
        if (!out_properties.returns_default_when_only_null && has_null_arguments)
            return nullptr;

        return found.creator(name, argument_types, parameters);
    }

    /// Combinators of aggregate functions.
    /// For every aggregate function 'agg' and combiner '-Comb' there is combined aggregate function with name 'aggComb',
    ///  that can have different number and/or types of arguments, different result type and different behaviour.

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        if (combinator->isForInternalUsageOnly())
            throw Exception("Aggregate function combinator '" + combinator->getName() + "' is only for internal usage", ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);

        String nested_name = name.substr(0, name.size() - combinator->getName().size());
        DataTypes nested_types = combinator->transformArguments(argument_types);
        Array nested_parameters = combinator->transformParameters(parameters);

        AggregateFunctionPtr nested_function = get(nested_name, nested_types, nested_parameters, out_properties, recursion_level + 1);
        return combinator->transformAggregateFunction(nested_function, out_properties, argument_types, parameters);
    }

    auto hints = this->getHints(name);
    if (!hints.empty())
        throw Exception(fmt::format("Unknown aggregate function {}. Maybe you meant: {}", name, toString(hints)),
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
    else
        throw Exception(fmt::format("Unknown aggregate function {}", name), ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::tryGet(
    const String & name, const DataTypes & argument_types, const Array & parameters, AggregateFunctionProperties & out_properties) const
{
    return isAggregateFunctionName(name)
        ? get(name, argument_types, parameters, out_properties)
        : nullptr;
}


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name, int recursion_level) const
{
    if (aggregate_functions.count(name) || isAlias(name))
        return true;

    String name_lowercase = Poco::toLower(name);
    if (recursion_level == 0 && (case_insensitive_aggregate_functions.count(name_lowercase) || isAlias(name_lowercase)))
        return true;

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
        return isAggregateFunctionName(name.substr(0, name.size() - combinator->getName().size()), recursion_level + 1);

    return false;
}

AggregateFunctionFactory & AggregateFunctionFactory::instance()
{
    static AggregateFunctionFactory ret;
    return ret;
}

}

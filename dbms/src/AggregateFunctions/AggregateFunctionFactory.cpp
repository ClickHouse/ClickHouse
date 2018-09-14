#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>

#include <Poco/String.h>
#include <DataTypes/DataTypeWithDictionary.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int LOGICAL_ERROR;
}


void AggregateFunctionFactory::registerFunction(const String & name, Creator creator, CaseSensitiveness case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception("AggregateFunctionFactory: the aggregate function " + name + " has been provided "
            " a null constructor", ErrorCodes::LOGICAL_ERROR);

    if (!aggregate_functions.emplace(name, creator).second)
        throw Exception("AggregateFunctionFactory: the aggregate function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_aggregate_functions.emplace(Poco::toLower(name), creator).second)
        throw Exception("AggregateFunctionFactory: the case insensitive aggregate function name '" + name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

static DataTypes convertTypesWithDictionaryToNested(const DataTypes & types)
{
    DataTypes res_types;
    res_types.reserve(types.size());
    for (const auto & type : types)
    {
        if (auto * type_with_dict = typeid_cast<const DataTypeWithDictionary *>(type.get()))
            res_types.push_back(type_with_dict->getDictionaryType());
        else
            res_types.push_back(type);
    }

    return res_types;
}

AggregateFunctionPtr AggregateFunctionFactory::get(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    int recursion_level) const
{
    auto type_without_dictionary = convertTypesWithDictionaryToNested(argument_types);

    /// If one of types is Nullable, we apply aggregate function combinator "Null".

    if (std::any_of(argument_types.begin(), argument_types.end(),
        [](const auto & type) { return type->isNullable(); }))
    {
        AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix("Null");
        if (!combinator)
            throw Exception("Logical error: cannot find aggregate function combinator to apply a function to Nullable arguments.", ErrorCodes::LOGICAL_ERROR);

        DataTypes nested_types = combinator->transformArguments(type_without_dictionary);

        AggregateFunctionPtr nested_function;

        /// A little hack - if we have NULL arguments, don't even create nested function.
        /// Combinator will check if nested_function was created.
        if (name == "count" || std::none_of(argument_types.begin(), argument_types.end(),
            [](const auto & type) { return type->onlyNull(); }))
            nested_function = getImpl(name, nested_types, parameters, recursion_level);

        return combinator->transformAggregateFunction(nested_function, argument_types, parameters);
    }

    auto res = getImpl(name, type_without_dictionary, parameters, recursion_level);
    if (!res)
        throw Exception("Logical error: AggregateFunctionFactory returned nullptr", ErrorCodes::LOGICAL_ERROR);
    return res;
}


AggregateFunctionPtr AggregateFunctionFactory::getImpl(
    const String & name_param,
    const DataTypes & argument_types,
    const Array & parameters,
    int recursion_level) const
{
    String name = getAliasToOrName(name_param);
    /// Find by exact match.
    auto it = aggregate_functions.find(name);
    if (it != aggregate_functions.end())
        return it->second(name, argument_types, parameters);

    /// Find by case-insensitive name.
    /// Combinators cannot apply for case insensitive (SQL-style) aggregate function names. Only for native names.
    if (recursion_level == 0)
    {
        auto it = case_insensitive_aggregate_functions.find(Poco::toLower(name));
        if (it != case_insensitive_aggregate_functions.end())
            return it->second(name, argument_types, parameters);
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
        AggregateFunctionPtr nested_function = get(nested_name, nested_types, parameters, recursion_level + 1);
        return combinator->transformAggregateFunction(nested_function, argument_types, parameters);
    }

    throw Exception("Unknown aggregate function " + name, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::tryGet(const String & name, const DataTypes & argument_types, const Array & parameters) const
{
    return isAggregateFunctionName(name)
        ? get(name, argument_types, parameters)
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

}

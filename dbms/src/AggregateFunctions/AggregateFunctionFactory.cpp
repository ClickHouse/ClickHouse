#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>

#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{

/// Does not check anything.
std::string trimRight(const std::string & in, const char * suffix)
{
    return in.substr(0, in.size() - strlen(suffix));
}

}

AggregateFunctionPtr createAggregateFunctionArray(AggregateFunctionPtr & nested, const DataTypes & argument_types);
AggregateFunctionPtr createAggregateFunctionForEach(AggregateFunctionPtr & nested, const DataTypes & argument_types);
AggregateFunctionPtr createAggregateFunctionIf(AggregateFunctionPtr & nested, const DataTypes & argument_types);
AggregateFunctionPtr createAggregateFunctionState(AggregateFunctionPtr & nested, const DataTypes & argument_types, const Array & parameters);
AggregateFunctionPtr createAggregateFunctionMerge(const String & name, AggregateFunctionPtr & nested, const DataTypes & argument_types);

AggregateFunctionPtr createAggregateFunctionNullUnary(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionNullVariadic(AggregateFunctionPtr & nested, const DataTypes & argument_types);
AggregateFunctionPtr createAggregateFunctionCountNotNull(const String & name, const DataTypes & argument_types, const Array & parameters);
AggregateFunctionPtr createAggregateFunctionNothing();


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


AggregateFunctionPtr AggregateFunctionFactory::get(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    int recursion_level) const
{
    bool has_nullable_types = false;
    bool has_null_types = false;
    for (const auto & arg_type : argument_types)
    {
        if (arg_type->isNullable())
        {
            has_nullable_types = true;
            if (arg_type->onlyNull())
            {
                has_null_types = true;
                break;
            }
        }
    }

    if (has_nullable_types)
    {
        /// Special case for 'count' function. It could be called with Nullable arguments
        /// - that means - count number of calls, when all arguments are not NULL.
        if (Poco::toLower(name) == "count")
            return createAggregateFunctionCountNotNull(name, argument_types, parameters);

        AggregateFunctionPtr nested_function;

        if (has_null_types)
        {
            nested_function = createAggregateFunctionNothing();
        }
        else
        {
            DataTypes nested_argument_types;
            nested_argument_types.reserve(argument_types.size());

            for (const auto & arg_type : argument_types)
                nested_argument_types.push_back(removeNullable(arg_type));

            nested_function = getImpl(name, nested_argument_types, parameters, recursion_level);
        }

        if (argument_types.size() == 1)
            return createAggregateFunctionNullUnary(nested_function);
        else
            return createAggregateFunctionNullVariadic(nested_function, argument_types);
    }
    else
        return getImpl(name, argument_types, parameters, recursion_level);
}


AggregateFunctionPtr AggregateFunctionFactory::getImpl(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    int recursion_level) const
{
    auto it = aggregate_functions.find(name);
    if (it != aggregate_functions.end())
    {
        auto it = aggregate_functions.find(name);
        if (it != aggregate_functions.end())
            return it->second(name, argument_types, parameters);
    }

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

    if (endsWith(name, "State"))
    {
        AggregateFunctionPtr nested = get(trimRight(name, "State"), argument_types, parameters, recursion_level + 1);
        return createAggregateFunctionState(nested, argument_types, parameters);
    }

    if (endsWith(name, "Merge"))
    {
        if (argument_types.size() != 1)
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument_types[0].get());
        if (!function)
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name
                + " must be AggregateFunction(...)", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        AggregateFunctionPtr nested = get(trimRight(name, "Merge"), function->getArgumentsDataTypes(), parameters, recursion_level + 1);

        if (nested->getName() != function->getFunctionName())
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name
                + ", because it corresponds to different aggregate function: " + function->getFunctionName() + " instead of " + nested->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return createAggregateFunctionMerge(name, nested, argument_types);
    }

    if (endsWith(name, "If"))
    {
        if (argument_types.empty())
            throw Exception("Incorrect number of arguments for aggregate function " + name,
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!typeid_cast<const DataTypeUInt8 *>(argument_types.back().get()))
            throw Exception("Illegal type " + argument_types.back()->getName() + " of last argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypes nested_dt = argument_types;
        nested_dt.pop_back();

        AggregateFunctionPtr nested = get(trimRight(name, "If"), nested_dt, parameters, recursion_level + 1);
        return createAggregateFunctionIf(nested, argument_types);
    }

    if (endsWith(name, "Array"))
    {
        DataTypes nested_arguments;
        for (const auto & type : argument_types)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception("Illegal type " + type->getName() + " of argument"
                    " for aggregate function " + name + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        AggregateFunctionPtr nested = get(trimRight(name, "Array"), nested_arguments, parameters, recursion_level + 1);
        return createAggregateFunctionArray(nested, argument_types);
    }

    if (endsWith(name, "ForEach"))
    {
        DataTypes nested_arguments;
        for (const auto & type : argument_types)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception("Illegal type " + type->getName() + " of argument"
                    " for aggregate function " + name + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        AggregateFunctionPtr nested = get(trimRight(name, "ForEach"), nested_arguments, parameters, recursion_level + 1);
        return createAggregateFunctionForEach(nested, argument_types);
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
    if (aggregate_functions.count(name))
        return true;

    if (recursion_level == 0 && case_insensitive_aggregate_functions.count(Poco::toLower(name)))
        return true;

    if (endsWith(name, "State"))
        return isAggregateFunctionName(trimRight(name, "State"), recursion_level + 1);
    if (endsWith(name, "Merge"))
        return isAggregateFunctionName(trimRight(name, "Merge"), recursion_level + 1);
    if (endsWith(name, "If"))
        return isAggregateFunctionName(trimRight(name, "If"), recursion_level + 1);
    if (endsWith(name, "Array"))
        return isAggregateFunctionName(trimRight(name, "Array"), recursion_level + 1);
    if (endsWith(name, "ForEach"))
        return isAggregateFunctionName(trimRight(name, "ForEach"), recursion_level + 1);

    return false;
}

}

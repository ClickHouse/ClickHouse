#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
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
}


namespace
{

/// Does not check anything.
std::string trimRight(const std::string & in, const char * suffix)
{
    return in.substr(0, in.size() - strlen(suffix));
}

}

AggregateFunctionPtr createAggregateFunctionArray(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionForEach(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionIf(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionState(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionMerge(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionNullUnary(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionNullVariadic(AggregateFunctionPtr & nested);
AggregateFunctionPtr createAggregateFunctionCountNotNull(const DataTypes & argument_types);


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
    for (const auto & arg_type : argument_types)
    {
        if (arg_type->isNullable() || arg_type->isNull())
        {
            has_nullable_types = true;
            break;
        }
    }

    if (has_nullable_types)
    {
        /// Special case for 'count' function. It could be called with Nullable arguments
        /// - that means - count number of calls, when all arguments are not NULL.
        if (Poco::toLower(name) == "count")
            return createAggregateFunctionCountNotNull(argument_types);

        DataTypes nested_argument_types;
        nested_argument_types.reserve(argument_types.size());

        for (const auto & arg_type : argument_types)
        {
            if (arg_type->isNullable())
            {
                const DataTypeNullable & actual_type = static_cast<const DataTypeNullable &>(*arg_type.get());
                const DataTypePtr & nested_type = actual_type.getNestedType();
                nested_argument_types.push_back(nested_type);
            }
            else
                nested_argument_types.push_back(arg_type);
        }

        AggregateFunctionPtr function = getImpl(name, nested_argument_types, parameters, recursion_level);

        if (argument_types.size() == 1)
            return createAggregateFunctionNullUnary(function);
        else
            return createAggregateFunctionNullVariadic(function);
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

    if (recursion_level == 0)
    {
        auto it = case_insensitive_aggregate_functions.find(Poco::toLower(name));
        if (it != case_insensitive_aggregate_functions.end())
            return it->second(name, argument_types, parameters);
    }

    if ((recursion_level == 0) && endsWith(name, "State"))
    {
        /// For aggregate functions of the form `aggState`, where `agg` is the name of another aggregate function.
        AggregateFunctionPtr nested = get(trimRight(name, "State"), argument_types, parameters, recursion_level + 1);
        return createAggregateFunctionState(nested);
    }

    if ((recursion_level <= 1) && endsWith(name, "Merge"))
    {
        /// For aggregate functions of the form `aggMerge`, where `agg` is the name of another aggregate function.
        if (argument_types.size() != 1)
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(&*argument_types[0]);
        if (!function)
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        AggregateFunctionPtr nested = get(trimRight(name, "Merge"), function->getArgumentsDataTypes(), parameters, recursion_level + 1);

        if (nested->getName() != function->getFunctionName())
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return createAggregateFunctionMerge(nested);
    }

    if ((recursion_level <= 2) && endsWith(name, "If"))
    {
        if (argument_types.empty())
            throw Exception{
                "Incorrect number of arguments for aggregate function " + name,
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        /// For aggregate functions of the form `aggIf`, where `agg` is the name of another aggregate function.
        DataTypes nested_dt = argument_types;
        nested_dt.pop_back();
        AggregateFunctionPtr nested = get(trimRight(name, "If"), nested_dt, parameters, recursion_level + 1);
        return createAggregateFunctionIf(nested);
    }

    if ((recursion_level <= 3) && endsWith(name, "Array"))
    {
        /// For aggregate functions of the form `aggArray`, where `agg` is the name of another aggregate function.
        size_t num_agruments = argument_types.size();

        DataTypes nested_arguments;
        for (size_t i = 0; i < num_agruments; ++i)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(&*argument_types[i]))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception("Illegal type " + argument_types[i]->getName() + " of argument #" + toString(i + 1) +
                    " for aggregate function " + name + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        /// + 3, so that no other modifier can go before the `Array`
        AggregateFunctionPtr nested = get(trimRight(name, "Array"), nested_arguments, parameters, recursion_level + 3);
        return createAggregateFunctionArray(nested);
    }

    if ((recursion_level <= 3) && endsWith(name, "ForEach"))
    {
        /// For functions like aggForEach, where 'agg' is the name of another aggregate function
        if (argument_types.size() != 1)
            throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_arguments;
        if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(&*argument_types[0]))
            nested_arguments.push_back(array->getNestedType());
        else
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        /// + 3, so that no other modifier can stay before ForEach. Note that the modifiers Array and ForEach are mutually exclusive.
        AggregateFunctionPtr nested = get(trimRight(name, "ForEach"), nested_arguments, parameters, recursion_level + 3);
        return createAggregateFunctionForEach(nested);
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

    /// For aggregate functions of the form `aggState`, where `agg` is the name of another aggregate function.
    if ((recursion_level <= 0) && endsWith(name, "State"))
        return isAggregateFunctionName(trimRight(name, "State"), recursion_level + 1);

    /// For aggregate functions of the form `aggMerge`, where `agg` is the name of another aggregate function.
    if ((recursion_level <= 1) && endsWith(name, "Merge"))
        return isAggregateFunctionName(trimRight(name, "Merge"), recursion_level + 1);

    /// For aggregate functions of the form `aggIf`, where `agg` is the name of another aggregate function.
    if ((recursion_level <= 2) && endsWith(name, "If"))
        return isAggregateFunctionName(trimRight(name, "If"), recursion_level + 1);

    /// For aggregate functions of the form `aggArray`, where `agg` is the name of another aggregate function.
    if ((recursion_level <= 3) && endsWith(name, "Array"))
    {
        /// + 3, so that no other modifier can go before `Array`
        return isAggregateFunctionName(trimRight(name, "Array"), recursion_level + 3);
    }

    if ((recursion_level <= 3) && endsWith(name, "ForEach"))
    {
        /// + 3, so that no other modifier can go before `ForEach`
        return isAggregateFunctionName(trimRight(name, "ForEach"), recursion_level + 3);
    }

    return false;
}

}

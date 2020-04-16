#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumMap.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include "registerAggregateFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct WithOverflowPolicy
{
    /// Overflow, meaning that the returned type is the same as the input type.
    static DataTypePtr promoteType(const DataTypePtr & data_type) { return data_type; }
};

struct WithoutOverflowPolicy
{
    /// No overflow, meaning we promote the types if necessary.
    static DataTypePtr promoteType(const DataTypePtr & data_type)
    {
        if (!data_type->canBePromoted())
            throw Exception{"Values to be summed are expected to be Numeric, Float or Decimal.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return data_type->promoteNumericType();
    }
};

template <typename T>
using SumMapWithOverflow = AggregateFunctionSumMap<T, WithOverflowPolicy>;

template <typename T>
using SumMapWithoutOverflow = AggregateFunctionSumMap<T, WithoutOverflowPolicy>;

template <typename T>
using SumMapFilteredWithOverflow = AggregateFunctionSumMapFiltered<T, WithOverflowPolicy>;

template <typename T>
using SumMapFilteredWithoutOverflow = AggregateFunctionSumMapFiltered<T, WithoutOverflowPolicy>;

using SumMapArgs = std::pair<DataTypePtr, DataTypes>;

SumMapArgs parseArguments(const std::string & name, const DataTypes & arguments)
{
    DataTypes args;

    if (arguments.size() == 1)
    {
        // sumMap is a transitive function, so it can be stored in SimpleAggregateFunction columns.
        // There is a caveat: it must support sumMap(sumMap(...)), e.g. it must be able to accept its
        // own output as an input. This is why we also support Tuple(keys, values) as an argument. 
        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        if (!tuple_type)
            throw Exception("When function " + name + " gets one argument it must be a tuple",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto elems = tuple_type->getElements();
        args.insert(args.end(), elems.begin(), elems.end());
    }
    else
        args.insert(args.end(), arguments.begin(), arguments.end());

    if (args.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments of Array type or one argument of tuple of two arrays",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * array_type = checkAndGetDataType<DataTypeArray>(args[0].get());
    if (!array_type)
        throw Exception("First argument for function " + name + " must be an array, not " + args[0]->getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    DataTypePtr keys_type = array_type->getNestedType();

    DataTypes values_types;
    values_types.reserve(args.size() - 1);
    for (size_t i = 1; i < args.size(); ++i)
    {
        array_type = checkAndGetDataType<DataTypeArray>(args[i].get());
        if (!array_type)
            throw Exception("Argument #" + toString(i) + " for function " + name + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        values_types.push_back(array_type->getNestedType());
    }

    return  {std::move(keys_type), std::move(values_types)};
}

template <template <typename> class Function>
AggregateFunctionPtr createAggregateFunctionSumMap(const std::string & name, const DataTypes & arguments, const Array & params)
{
    assertNoParameters(name, params);

    auto [keys_type, values_types] = parseArguments(name, arguments);

    AggregateFunctionPtr res(createWithNumericBasedType<Function>(*keys_type, keys_type, values_types, arguments));
    if (!res)
        res.reset(createWithDecimalType<Function>(*keys_type, keys_type, values_types, arguments));
    if (!res)
        res.reset(createWithStringType<Function>(*keys_type, keys_type, values_types, arguments));
    if (!res)
        throw Exception("Illegal type of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

template <template <typename> class Function>
AggregateFunctionPtr createAggregateFunctionSumMapFiltered(const std::string & name, const DataTypes & arguments, const Array & params)
{
    if (params.size() != 1)
        throw Exception("Aggregate function " + name + " requires exactly one parameter of Array type.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Array keys_to_keep;
    if (!params.front().tryGet<Array>(keys_to_keep))
        throw Exception("Aggregate function " + name + " requires an Array as parameter.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto [keys_type, values_types] = parseArguments(name, arguments);

    AggregateFunctionPtr res(createWithNumericBasedType<Function>(*keys_type, keys_type, values_types, keys_to_keep, arguments, params));
    if (!res)
        res.reset(createWithDecimalType<Function>(*keys_type, keys_type, values_types, keys_to_keep, arguments, params));
    if (!res)
        res.reset(createWithStringType<Function>(*keys_type, keys_type, values_types, keys_to_keep, arguments, params));
    if (!res)
        throw Exception("Illegal type of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}
}

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumMap", createAggregateFunctionSumMap<SumMapWithoutOverflow>);
    factory.registerFunction("sumMapWithOverflow", createAggregateFunctionSumMap<SumMapWithOverflow>);
    factory.registerFunction("sumMapFiltered", createAggregateFunctionSumMapFiltered<SumMapFilteredWithoutOverflow>);
    factory.registerFunction("sumMapFilteredWithOverflow", createAggregateFunctionSumMapFiltered<SumMapFilteredWithOverflow>);
}

}

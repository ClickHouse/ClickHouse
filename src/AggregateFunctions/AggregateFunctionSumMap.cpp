#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumMap.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

auto parseArguments(const std::string & name, const DataTypes & arguments)
{
    DataTypes args;
    bool tuple_argument = false;

    if (arguments.size() == 1)
    {
        // sumMap state is fully given by its result, so it can be stored in
        // SimpleAggregateFunction columns. There is a caveat: it must support
        // sumMap(sumMap(...)), e.g. it must be able to accept its own output as
        // an input. This is why it also accepts a Tuple(keys, values) argument.
        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        if (!tuple_type)
            throw Exception("When function " + name + " gets one argument it must be a tuple",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto elems = tuple_type->getElements();
        args.insert(args.end(), elems.begin(), elems.end());
        tuple_argument = true;
    }
    else
    {
        args.insert(args.end(), arguments.begin(), arguments.end());
        tuple_argument = false;
    }

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

    return std::tuple<DataTypePtr, DataTypes, bool>{std::move(keys_type), std::move(values_types), tuple_argument};
}

// This function instantiates a particular overload of the sumMap family of
// functions.
// The template parameter MappedFunction<bool template_argument> is an aggregate
// function template that allows to choose the aggregate function variant that
// accepts either normal arguments or tuple argument.
template<template <bool tuple_argument> typename MappedFunction>
AggregateFunctionPtr createAggregateFunctionMap(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);

    AggregateFunctionPtr res;
    if (tuple_argument)
    {
        res.reset(createWithNumericBasedType<MappedFunction<true>::template F>(*keys_type, keys_type, values_types, arguments, params));
        if (!res)
            res.reset(createWithDecimalType<MappedFunction<true>::template F>(*keys_type, keys_type, values_types, arguments, params));
        if (!res)
            res.reset(createWithStringType<MappedFunction<true>::template F>(*keys_type, keys_type, values_types, arguments, params));
    }
    else
    {
        res.reset(createWithNumericBasedType<MappedFunction<false>::template F>(*keys_type, keys_type, values_types, arguments, params));
        if (!res)
            res.reset(createWithDecimalType<MappedFunction<false>::template F>(*keys_type, keys_type, values_types, arguments, params));
        if (!res)
            res.reset(createWithStringType<MappedFunction<false>::template F>(*keys_type, keys_type, values_types, arguments, params));
    }
    if (!res)
        throw Exception("Illegal type of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

// This template chooses the sumMap variant with given filtering and overflow
// handling.
template <bool filtered, bool overflow>
struct SumMapVariants
{
    // SumMapVariants chooses the `overflow` and `filtered` parameters of the
    // aggregate functions. The `tuple_argument` and the value type `T` are left
    // as free parameters.
    // DispatchOnTupleArgument chooses `tuple_argument`, and the value type `T`
    // is left free.
    template <bool tuple_argument>
    struct DispatchOnTupleArgument
    {
        template <typename T>
        using F = std::conditional_t<filtered,
            AggregateFunctionSumMapFiltered<T, overflow, tuple_argument>,
            AggregateFunctionSumMap<T, overflow, tuple_argument>>;
    };
};

// This template gives an aggregate function template that is narrowed
// to accept either tuple argumen or normal arguments.
template <bool tuple_argument>
struct MinMapDispatchOnTupleArgument
{
    template <typename T>
    using F = AggregateFunctionMinMap<T, tuple_argument>;
};

// This template gives an aggregate function template that is narrowed
// to accept either tuple argumen or normal arguments.
template <bool tuple_argument>
struct MaxMapDispatchOnTupleArgument
{
    template <typename T>
    using F = AggregateFunctionMaxMap<T, tuple_argument>;
};

}

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    // these functions used to be called *Map, with now these names occupied by
    // Map combinator, which redirects calls here if was called with
    // array or tuple arguments.
    factory.registerFunction("sumMappedArrays", createAggregateFunctionMap<
        SumMapVariants<false, false>::DispatchOnTupleArgument>);

    factory.registerFunction("minMappedArrays",
        createAggregateFunctionMap<MinMapDispatchOnTupleArgument>);

    factory.registerFunction("maxMappedArrays",
        createAggregateFunctionMap<MaxMapDispatchOnTupleArgument>);

    // these functions could be renamed to *MappedArrays too, but it would
    // break backward compatibility
    factory.registerFunction("sumMapWithOverflow", createAggregateFunctionMap<
        SumMapVariants<false, true>::DispatchOnTupleArgument>);

    factory.registerFunction("sumMapFiltered", createAggregateFunctionMap<
        SumMapVariants<true, false>::DispatchOnTupleArgument>);

    factory.registerFunction("sumMapFilteredWithOverflow",
        createAggregateFunctionMap<
            SumMapVariants<true, true>::DispatchOnTupleArgument>);
}

}

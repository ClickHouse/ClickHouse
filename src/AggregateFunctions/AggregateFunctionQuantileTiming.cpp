#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileTiming.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool _> using FuncQuantileTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTiming, false, Float32, false, false>;
template <typename Value, bool _> using FuncQuantilesTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTiming, false, Float32, true, false>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} requires at least one argument", name);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE, true>>(argument_types, params);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileTiming(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    factory.registerFunction(NameQuantileTiming::name, createAggregateFunctionQuantile<FuncQuantileTiming>);
    factory.registerFunction(NameQuantilesTiming::name, { createAggregateFunctionQuantile<FuncQuantilesTiming>, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianTiming", NameQuantileTiming::name);
}

}

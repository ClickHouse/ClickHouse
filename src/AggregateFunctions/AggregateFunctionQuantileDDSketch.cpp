#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileDDSketch.h>
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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool float_return> using FuncQuantileDDSketch = AggregateFunctionQuantile<Value, QuantileDDSketch<Value>, NameQuantileDDSketch, false, std::conditional_t<float_return, Float64, void>, false, true>;
template <typename Value, bool float_return> using FuncQuantilesDDSketch = AggregateFunctionQuantile<Value, QuantileDDSketch<Value>, NameQuantilesDDSketch, false, std::conditional_t<float_return, Float64, void>, true, true>;


template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    /// Second argument type check doesn't depend on the type of the first one.
    Function<void, true>::assertSecondArg(argument_types);

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

void registerAggregateFunctionsQuantileDDSketch(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    factory.registerFunction(NameQuantileDDSketch::name, createAggregateFunctionQuantile<FuncQuantileDDSketch>);
    factory.registerFunction(NameQuantilesDDSketch::name, { createAggregateFunctionQuantile<FuncQuantilesDDSketch>, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianDDSketch", NameQuantileDDSketch::name);
}

}

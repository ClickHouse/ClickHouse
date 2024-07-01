#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileBFloat16Histogram.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
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

template <typename Value, bool float_return> using FuncQuantileBFloat16Weighted = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantileBFloat16Weighted, true, std::conditional_t<float_return, Float64, void>, false, false>;
template <typename Value, bool float_return> using FuncQuantilesBFloat16Weighted = AggregateFunctionQuantile<Value, QuantileBFloat16Histogram<Value>, NameQuantilesBFloat16Weighted, true, std::conditional_t<float_return, Float64, void>, true, false>;

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

void registerAggregateFunctionsQuantileBFloat16Weighted(AggregateFunctionFactory & factory)
{
    factory.registerFunction(NameQuantileBFloat16Weighted::name, createAggregateFunctionQuantile<FuncQuantileBFloat16Weighted>);
    factory.registerFunction(NameQuantilesBFloat16Weighted::name, createAggregateFunctionQuantile<FuncQuantilesBFloat16Weighted>);

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianBFloat16Weighted", NameQuantileBFloat16Weighted::name);
}

}

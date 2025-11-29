#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesCoalesceGridValues.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}


namespace
{
    using Mode = AggregateFunctionTimeSeriesCoalesceGridValuesMode;

    template <typename ValueType>
    AggregateFunctionPtr createWithValueType(const DataTypes & argument_types, Mode mode)
    {
        return std::make_shared<AggregateFunctionTimeSeriesCoalesceGridValues<ValueType>>(argument_types, mode);
    }

    Mode parseParameterMode(std::string_view function_name, std::string_view parameter_name, const Field & parameter_field)
    {
        if (String string_value; parameter_field.tryGet(string_value))
        {
            if (string_value == "null_if_conflict")
                return Mode::NULL_IF_CONFLICT;
            else if (string_value == "throw_if_conflict")
                return Mode::THROW_IF_CONFLICT;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot parse {} parameter for aggregate function {}", parameter_name, function_name);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of {} parameter for aggregate function {}",
                parameter_field.getTypeName(), parameter_name, function_name);
        }
    }

    AggregateFunctionPtr createAggregateFunctionTimeSeriesCoalesceGridValues(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        if (parameters.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires 1 parameter: mode", name);

        Mode mode = parseParameterMode(name, "mode", parameters[0]);

        assertUnary(name, argument_types);

        if (argument_types[0]->getTypeId() != TypeIndex::Array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects one argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());

        const auto & nullable_type = typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType();

        if (nullable_type->getTypeId() != TypeIndex::Nullable)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects one argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());

        const auto & value_type = typeid_cast<const DataTypeNullable *>(nullable_type.get())->getNestedType();

        AggregateFunctionPtr res;
        if (value_type->getTypeId() == TypeIndex::Float64)
        {
            res = createWithValueType<Float64>(argument_types, mode);
        }
        else if (value_type->getTypeId() == TypeIndex::Float32)
        {
            res = createWithValueType<Float32>(argument_types, mode);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects one argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());
        }

        return res;
    }
}

void registerAggregateFunctionTimeSeriesCoalesceGridValues(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesCoalesceGridValues", createAggregateFunctionTimeSeriesCoalesceGridValues);
}

}

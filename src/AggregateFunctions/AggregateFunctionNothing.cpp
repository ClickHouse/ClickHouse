#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct Settings;

void registerAggregateFunctionNothing(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false };
    auto create_aggregate_function = [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        if (!parameters.empty())
        {
            if (parameters[0].getType() != Field::Types::String)
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Aggregate function {} requires parameter to be String, got {}",
                    name, parameters[0].getTypeName());
            }
            auto type_string = parameters[0].safeGet<String>();
            auto result_type = DataTypeFactory::instance().get(type_string);
            if (!result_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Aggregate function {} requires parameter to be a valid data type, got {}",
                    name, type_string);
            return std::make_shared<AggregateFunctionNothing>(argument_types, parameters, result_type);
        }

        auto result_type = argument_types.empty() ? std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()) : argument_types.front();
        return std::make_shared<AggregateFunctionNothing>(argument_types, parameters, result_type);
    };

    factory.registerFunction("nothing", {create_aggregate_function, properties});
}

}

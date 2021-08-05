#include "AggregateFunctionMap.h"
#include "AggregateFunctions/AggregateFunctionCombinatorFactory.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{
class AggregateFunctionCombinatorMap final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Map"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with " + getName() + " suffix");

        const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].get());
        if (map_type)
            return DataTypes({map_type->getValueType()});

        // we need this part just to pass to redirection for mapped arrays
        const auto * tup_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        if (tup_type)
        {
            const auto * val_array_type = checkAndGetDataType<DataTypeArray>(tup_type->getElements()[1].get());
            if (val_array_type)
                return DataTypes({val_array_type->getNestedType()});
        }

        if (arguments.size() >= 2)
        {
            const auto * val_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
            if (val_array_type)
                return DataTypes({val_array_type->getNestedType()});
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function " + getName() + " requires map as argument");
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].get());
        if (map_type)
        {
            auto key_type_id = map_type->getKeyType()->getTypeId();

            switch (key_type_id)
            {
                case TypeIndex::Enum8:
                case TypeIndex::Int8:
                    return std::make_shared<AggregateFunctionMap<Int8>>(nested_function, arguments);
                case TypeIndex::Enum16:
                case TypeIndex::Int16:
                    return std::make_shared<AggregateFunctionMap<Int16>>(nested_function, arguments);
                case TypeIndex::Int32:
                    return std::make_shared<AggregateFunctionMap<Int32>>(nested_function, arguments);
                case TypeIndex::Int64:
                    return std::make_shared<AggregateFunctionMap<Int64>>(nested_function, arguments);
                case TypeIndex::UInt8:
                    return std::make_shared<AggregateFunctionMap<UInt8>>(nested_function, arguments);
                case TypeIndex::Date:
                case TypeIndex::UInt16:
                    return std::make_shared<AggregateFunctionMap<UInt16>>(nested_function, arguments);
                case TypeIndex::DateTime:
                case TypeIndex::UInt32:
                    return std::make_shared<AggregateFunctionMap<UInt32>>(nested_function, arguments);
                case TypeIndex::UInt64:
                    return std::make_shared<AggregateFunctionMap<UInt64>>(nested_function, arguments);
                case TypeIndex::UUID:
                    return std::make_shared<AggregateFunctionMap<UInt128>>(nested_function, arguments);
                case TypeIndex::FixedString:
                case TypeIndex::String:
                    return std::make_shared<AggregateFunctionMap<String>>(nested_function, arguments);
                default:
                    throw Exception{"Illegal columns in arguments for combinator " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }
        }
        else if (!arguments.empty())
        {
            // check if we got tuple of arrays or just arrays and if so, try to redirect to sum/min/max-MappedArrays to implement old behavior
            auto nested_func_name = nested_function->getName();
            if (nested_func_name == "sum" || nested_func_name == "min" || nested_func_name == "max")
            {
                bool match;
                const auto * tup_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
                auto check_func = [](DataTypePtr t) {
                    return t->getTypeId() == TypeIndex::Array;
                };

                if (tup_type)
                {
                    const auto & types = tup_type->getElements();
                    match = arguments.size() == 1 && types.size() >= 2 && std::all_of(types.begin(), types.end(), check_func);
                }
                else
                {
                    // sumMappedArrays and others support more than 2 mapped arrays
                    match = arguments.size() >= 2 && std::all_of(arguments.begin(), arguments.end(), check_func);
                }

                if (match)
                {
                    AggregateFunctionProperties out_properties;
                    auto & aggr_func_factory = AggregateFunctionFactory::instance();
                    return aggr_func_factory.get(nested_func_name + "MappedArrays", arguments, params, out_properties);
                }
            }
        }

        throw Exception{"Illegal columns in arguments for combinator " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

void registerAggregateFunctionCombinatorMap(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMap>());
}

}

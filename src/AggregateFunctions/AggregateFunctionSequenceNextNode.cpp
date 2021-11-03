#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceNextNode.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <base/range.h>


namespace DB
{

constexpr size_t max_events_size = 64;

constexpr size_t min_required_args = 3;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace
{

template <typename T>
inline AggregateFunctionPtr createAggregateFunctionSequenceNodeImpl(
    const DataTypePtr data_type, const DataTypes & argument_types, const Array & parameters, SequenceDirection direction, SequenceBase base)
{
    return std::make_shared<SequenceNextNodeImpl<T, NodeString<max_events_size>>>(
        data_type, argument_types, parameters, base, direction, min_required_args);
}

AggregateFunctionPtr
createAggregateFunctionSequenceNode(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings == nullptr || !settings->allow_experimental_funnel_functions)
    {
        throw Exception(
            "Aggregate function " + name + " is experimental. Set `allow_experimental_funnel_functions` setting to enable it",
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
    }

    if (parameters.size() < 2)
        throw Exception("Aggregate function '" + name + "' requires 2 parameters (direction, head)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    auto expected_param_type = Field::Types::Which::String;
    if (parameters.at(0).getType() != expected_param_type || parameters.at(1).getType() != expected_param_type)
        throw Exception("Aggregate function '" + name + "' requires 'String' parameters",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    String param_dir = parameters.at(0).safeGet<String>();
    std::unordered_map<std::string, SequenceDirection> seq_dir_mapping{
        {"forward", SequenceDirection::Forward},
        {"backward", SequenceDirection::Backward},
    };
    if (!seq_dir_mapping.contains(param_dir))
        throw Exception{"Aggregate function " + name + " doesn't support a parameter: " + param_dir, ErrorCodes::BAD_ARGUMENTS};
    SequenceDirection direction = seq_dir_mapping[param_dir];

    String param_base = parameters.at(1).safeGet<String>();
    std::unordered_map<std::string, SequenceBase> seq_base_mapping{
        {"head", SequenceBase::Head},
        {"tail", SequenceBase::Tail},
        {"first_match", SequenceBase::FirstMatch},
        {"last_match", SequenceBase::LastMatch},
    };
    if (!seq_base_mapping.contains(param_base))
        throw Exception{"Aggregate function " + name + " doesn't support a parameter: " + param_base, ErrorCodes::BAD_ARGUMENTS};
    SequenceBase base = seq_base_mapping[param_base];

    if ((base == SequenceBase::Head && direction == SequenceDirection::Backward) ||
        (base == SequenceBase::Tail && direction == SequenceDirection::Forward))
        throw Exception(fmt::format(
            "Invalid argument combination of '{}' with '{}'", param_base, param_dir), ErrorCodes::BAD_ARGUMENTS);

    if (argument_types.size() < min_required_args)
        throw Exception("Aggregate function " + name + " requires at least " + toString(min_required_args) + " arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    bool is_base_match_type = base == SequenceBase::FirstMatch || base == SequenceBase::LastMatch;
    if (is_base_match_type && argument_types.size() < min_required_args + 1)
        throw Exception(
            "Aggregate function " + name + " requires at least " + toString(min_required_args + 1) + " arguments when base is first_match or last_match.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() > max_events_size + min_required_args)
        throw Exception(fmt::format(
            "Aggregate function '{}' requires at most {} (timestamp, value_column, ...{} events) arguments.",
                name, max_events_size + min_required_args, max_events_size), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (const auto * cond_arg = argument_types[2].get(); cond_arg && !isUInt8(cond_arg))
        throw Exception("Illegal type " + cond_arg->getName() + " of third argument of aggregate function "
                + name + ", must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    for (const auto i : collections::range(min_required_args, argument_types.size()))
    {
        const auto * cond_arg = argument_types[i].get();
        if (!isUInt8(cond_arg))
            throw Exception(fmt::format(
                "Illegal type '{}' of {} argument of aggregate function '{}', must be UInt8", cond_arg->getName(), i + 1, name),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    if (WhichDataType(argument_types[1].get()).idx != TypeIndex::String)
        throw Exception{"Illegal type " + argument_types[1].get()->getName()
                + " of second argument of aggregate function " + name + ", must be String",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    DataTypePtr data_type = makeNullable(argument_types[1]);

    WhichDataType timestamp_type(argument_types[0].get());
    if (timestamp_type.idx == TypeIndex::UInt8)
        return createAggregateFunctionSequenceNodeImpl<UInt8>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt16)
        return createAggregateFunctionSequenceNodeImpl<UInt16>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt32)
        return createAggregateFunctionSequenceNodeImpl<UInt32>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt64)
        return createAggregateFunctionSequenceNodeImpl<UInt64>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.isDate())
        return createAggregateFunctionSequenceNodeImpl<DataTypeDate::FieldType>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.isDateTime())
        return createAggregateFunctionSequenceNodeImpl<DataTypeDateTime::FieldType>(data_type, argument_types, parameters, direction, base);

    throw Exception{"Illegal type " + argument_types.front().get()->getName()
            + " of first argument of aggregate function " + name + ", must be Unsigned Number, Date, DateTime",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

}

void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("sequenceNextNode", { createAggregateFunctionSequenceNode, properties });
}

}

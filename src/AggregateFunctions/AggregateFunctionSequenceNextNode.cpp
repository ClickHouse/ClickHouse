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
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Aggregate function {} is experimental. "
            "Set `allow_experimental_funnel_functions` setting to enable it", name);
    }

    if (parameters.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function '{}' requires 2 parameters (direction, head)", name);
    auto expected_param_type = Field::Types::Which::String;
    if (parameters.at(0).getType() != expected_param_type || parameters.at(1).getType() != expected_param_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function '{}' requires 'String' parameters", name);

    String param_dir = parameters.at(0).safeGet<String>();
    std::unordered_map<std::string, SequenceDirection> seq_dir_mapping{
        {"forward", SequenceDirection::Forward},
        {"backward", SequenceDirection::Backward},
    };
    if (!seq_dir_mapping.contains(param_dir))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", name, param_dir);
    SequenceDirection direction = seq_dir_mapping[param_dir];

    String param_base = parameters.at(1).safeGet<String>();
    std::unordered_map<std::string, SequenceBase> seq_base_mapping{
        {"head", SequenceBase::Head},
        {"tail", SequenceBase::Tail},
        {"first_match", SequenceBase::FirstMatch},
        {"last_match", SequenceBase::LastMatch},
    };
    if (!seq_base_mapping.contains(param_base))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", name, param_base);
    SequenceBase base = seq_base_mapping[param_base];

    if ((base == SequenceBase::Head && direction == SequenceDirection::Backward) ||
        (base == SequenceBase::Tail && direction == SequenceDirection::Forward))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid argument combination of '{}' with '{}'", param_base, param_dir);

    if (argument_types.size() < min_required_args)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function {} requires at least {} arguments.", name, toString(min_required_args));

    bool is_base_match_type = base == SequenceBase::FirstMatch || base == SequenceBase::LastMatch;
    if (is_base_match_type && argument_types.size() < min_required_args + 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires at least {} arguments when base is first_match or last_match.",
            name, toString(min_required_args + 1));

    if (argument_types.size() > max_events_size + min_required_args)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function '{}' requires at most {} (timestamp, value_column, ...{} events) arguments.",
                name, max_events_size + min_required_args, max_events_size);

    if (const auto * cond_arg = argument_types[2].get(); cond_arg && !isUInt8(cond_arg))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of aggregate function {}, "
                        "must be UInt8", cond_arg->getName(), name);

    for (const auto i : collections::range(min_required_args, argument_types.size()))
    {
        const auto * cond_arg = argument_types[i].get();
        if (!isUInt8(cond_arg))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type '{}' of {} argument of aggregate function '{}', must be UInt8", cond_arg->getName(), i + 1, name);
    }

    if (WhichDataType(argument_types[1].get()).idx != TypeIndex::String)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of second argument of aggregate function {}, must be String",
                        argument_types[1].get()->getName(), name);

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

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of aggregate function {}, must "
                    "be Unsigned Number, Date, DateTime", argument_types.front().get()->getName(), name);
}

}

void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("sequenceNextNode", { createAggregateFunctionSequenceNode, properties });
}

}

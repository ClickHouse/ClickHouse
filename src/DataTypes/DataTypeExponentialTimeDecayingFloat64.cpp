#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

String DataTypeExponentialTimeDecayingFloat64Name::getName() const
{
    WriteBufferFromOwnString out;
    out << "ExponentialTimeDecayingFloat64(" << time_type->getName() << ')';
    return out.str();
}

namespace
{

void assertTimeType(const DataTypePtr & time_type)
{
    if (!isNumber(time_type) && !isDateTime(time_type) && !isDateTime64(time_type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Time type of ExponentialTimeDecayingFloat64 must be a number, DateTime, or DateTime64, got {}",
            time_type->getName());
}

std::pair<DataTypePtr, DataTypeCustomDescPtr> create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Data type ExponentialTimeDecayingFloat64 takes exactly one argument: the time type");

    auto time_type = DataTypeFactory::instance().get(arguments->children[0]);
    assertTimeType(time_type);

    auto component_type = std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
        Names{"value", "half_life"});
    auto storage_type = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeFloat64>(),
            time_type,
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeArray>(component_type)},
        Names{"value", "time", "half_life", "components"});

    return {
        storage_type,
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeExponentialTimeDecayingFloat64Name>(std::move(time_type)))};
}

}

DataTypePtr createDataTypeExponentialTimeDecayingFloat64(const DataTypePtr & time_type)
{
    assertTimeType(time_type);

    auto custom_name = std::make_unique<DataTypeExponentialTimeDecayingFloat64Name>(time_type);
    const String base_name = "Tuple(value Float64, time " + time_type->getName()
        + ", half_life Float64, components Array(Tuple(value Float64, half_life Float64)))";
    return DataTypeFactory::instance().getCustom(
        base_name,
        std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
}

bool isExponentialTimeDecayingFloat64(const DataTypePtr & type)
{
    return type && typeid_cast<const DataTypeExponentialTimeDecayingFloat64Name *>(type->getCustomName());
}

const DataTypePtr & getExponentialTimeDecayingFloat64TimeType(const DataTypePtr & type)
{
    const auto * custom_name = type
        ? typeid_cast<const DataTypeExponentialTimeDecayingFloat64Name *>(type->getCustomName())
        : nullptr;
    if (!custom_name)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Data type {} is not ExponentialTimeDecayingFloat64",
            type ? type->getName() : "nullptr");
    return custom_name->getTimeType();
}

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom(
        "ExponentialTimeDecayingFloat64",
        create,
        DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"(
Represents one or more non-negative exponentially time-decaying values at a shared anchor time.

The public fields are `value`, `time`, and `half_life`. `value` is the sum of the component
values at `time`. The public `half_life` is
`sum(component.half_life * component.value) / value`. The internal `components` field preserves
the individual half-lives so values can be combined independently of evaluation order.
Use `tupleElement(decaying_value, 'time')` to read the greatest observed or current anchor time.
)",
            .syntax = "ExponentialTimeDecayingFloat64(time_type)",
            .examples = {},
            .related = {},
        });
}

}

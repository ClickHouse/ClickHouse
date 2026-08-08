#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTLiteral.h>

#include <cmath>
#include <fmt/format.h>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
}

namespace
{

Float64 getDecayLength(const ASTPtr & parameters)
{
    if (!parameters || parameters->children.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Data type ExponentialTimeDecayingFloat64 takes exactly one parameter, the decay length");

    const auto * literal = parameters->children[0]->as<ASTLiteral>();
    if (!literal)
        throw Exception(
            ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS,
            "Decay length of data type ExponentialTimeDecayingFloat64 must be a literal");

    const Float64 decay_length = applyVisitor(FieldVisitorConvertToNumber<Float64>(), literal->value);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Decay length of data type ExponentialTimeDecayingFloat64 must be finite and positive");

    return decay_length;
}

std::pair<DataTypePtr, DataTypeCustomDescPtr> create(Float64 decay_length)
{
    auto storage_type = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeFloat64>()},
        Names{"value", "time", "decay_length"});

    return {
        storage_type,
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomExponentialTimeDecayingFloat64>(decay_length))};
}

std::pair<DataTypePtr, DataTypeCustomDescPtr> create(const ASTPtr & parameters)
{
    return create(getDecayLength(parameters));
}

}

String DataTypeCustomExponentialTimeDecayingFloat64::getName() const
{
    return fmt::format("ExponentialTimeDecayingFloat64({})", decay_length);
}

DataTypePtr createDataTypeExponentialTimeDecayingFloat64(Float64 decay_length)
{
    return DataTypeFactory::instance().getCustom(
        "Tuple(value Float64, time Float64, decay_length Float64)",
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomExponentialTimeDecayingFloat64>(decay_length)));
}

std::optional<Float64> tryGetExponentialTimeDecayingFloat64DecayLength(const DataTypePtr & type)
{
    if (!type || !type->getCustomName())
        return std::nullopt;

    if (const auto * decaying_type
        = dynamic_cast<const DataTypeCustomExponentialTimeDecayingFloat64 *>(type->getCustomName()))
        return decaying_type->getDecayLength();

    if (const auto * simple_aggregate
        = dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
    {
        const auto & argument_types = simple_aggregate->getArgumentsDataTypes();
        if (argument_types.size() == 1)
            return tryGetExponentialTimeDecayingFloat64DecayLength(argument_types[0]);
    }

    return std::nullopt;
}

bool isExponentialTimeDecayingFloat64(const DataTypePtr & type)
{
    return tryGetExponentialTimeDecayingFloat64DecayLength(type).has_value();
}

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom(
        "ExponentialTimeDecayingFloat64",
        create,
        DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"(
Represents one or more finite exponentially time-decaying values at a shared anchor time.

The decay length is part of the type: `ExponentialTimeDecayingFloat64(decay_length)`.
The stored fields are `value Float64`, `time Float64`, and a redundant `decay_length Float64`
marker. DateTime and DateTime64 inputs are represented as seconds. The marker is validated against
the type parameter when a value is combined or evaluated, so incompatible decay lengths are not
silently mixed even in paths where ClickHouse treats custom tuple storage as layout-compatible.

Use `tupleElement(decaying_value, 'time')` to read the greatest observed or current anchor time.
The type can be used with `SimpleAggregateFunction(exponentialTimeDecayedSum, ...)` in an
`AggregatingMergeTree`.

Addition uses `Float64` arithmetic. Large signed values that nearly cancel can produce different
results when their order or grouping changes. Users who require stronger numerical reproducibility
should normalize magnitudes or pre-aggregate numerically sensitive inputs with a numerically stable
method before constructing or combining these values.
)",
            .syntax = "ExponentialTimeDecayingFloat64(decay_length)",
            .examples = {},
            .related = {"SimpleAggregateFunction"},
        });
}

}

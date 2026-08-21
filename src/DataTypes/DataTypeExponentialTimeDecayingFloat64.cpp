#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
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
        Names{"sign", "signed_unit_time", "decay_length"});

    return {
        storage_type,
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomExponentialTimeDecayingFloat64>(decay_length))};
}

std::pair<DataTypePtr, DataTypeCustomDescPtr> createFromParameters(const ASTPtr & parameters)
{
    return create(getDecayLength(parameters));
}

}

String DataTypeCustomExponentialTimeDecayingFloat64::getName() const
{
    return fmt::format("ExponentialTimeDecayingFloat64({})", decay_length);
}

std::optional<Field> DataTypeCustomExponentialTimeDecayingFloat64::getDefault() const
{
    return Tuple{Float64(0), Float64(0), decay_length};
}

DataTypePtr createDataTypeExponentialTimeDecayingFloat64(Float64 decay_length)
{
    return DataTypeFactory::instance().getCustom(
        "Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)",
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomExponentialTimeDecayingFloat64>(decay_length)));
}

std::optional<Float64> tryGetExponentialTimeDecayingFloat64DecayLength(const IDataType & type)
{
    if (!type.getCustomName())
        return std::nullopt;

    if (const auto * decaying_type
        = dynamic_cast<const DataTypeCustomExponentialTimeDecayingFloat64 *>(type.getCustomName()))
        return decaying_type->getDecayLength();

    if (const auto * simple_aggregate
        = dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(type.getCustomName()))
    {
        const auto & argument_types = simple_aggregate->getArgumentsDataTypes();
        if (argument_types.size() == 1)
            return tryGetExponentialTimeDecayingFloat64DecayLength(*argument_types[0]);
    }

    return std::nullopt;
}

std::optional<Float64> tryGetExponentialTimeDecayingFloat64DecayLength(const DataTypePtr & type)
{
    return type ? tryGetExponentialTimeDecayingFloat64DecayLength(*type) : std::nullopt;
}

bool isExponentialTimeDecayingFloat64(const IDataType & type)
{
    return tryGetExponentialTimeDecayingFloat64DecayLength(type).has_value();
}

bool isExponentialTimeDecayingFloat64(const DataTypePtr & type)
{
    return tryGetExponentialTimeDecayingFloat64DecayLength(type).has_value();
}

void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, Float64 decay_length, const String & operation)
{
    ColumnPtr full_column = column.convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();
    const ColumnNullable * nullable = typeid_cast<const ColumnNullable *>(full_column.get());

    ColumnPtr nested_holder;
    const IColumn * nested_column = full_column.get();
    if (nullable)
    {
        nested_holder = nullable->getNestedColumnPtr()->convertToFullColumnIfLowCardinality();
        nested_column = nested_holder.get();
    }

    const auto & tuple = assert_cast<const ColumnTuple &>(*nested_column);
    const auto & signs = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & signed_unit_times = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();
    const auto & stored_decay_lengths = assert_cast<const ColumnFloat64 &>(tuple.getColumn(2)).getData();

    for (size_t row = 0; row < tuple.size(); ++row)
    {
        if (nullable && nullable->isNullAt(row))
            continue;

        const Float64 stored_decay_length = stored_decay_lengths[row];
        if (!std::isfinite(stored_decay_length) || stored_decay_length != decay_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecayingFloat64 value in {}: stored decay length {} does not match type decay length {}",
                operation,
                stored_decay_length,
                decay_length);

        const Float64 sign = signs[row];
        const Float64 signed_unit_time = signed_unit_times[row];
        if (sign == 0)
        {
            if (signed_unit_time != 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Malformed ExponentialTimeDecayingFloat64 value in {}: zero value must have zero signed unit time",
                    operation);
        }
        else if ((sign != -1 && sign != 1) || !std::isfinite(signed_unit_time))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecayingFloat64 value in {}: expected a canonical sign and finite signed unit time",
                operation);
        }
    }
}

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom(
        "ExponentialTimeDecayingFloat64",
        createFromParameters,
        DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"(
Represents one or more finite exponentially time-decaying values at a shared anchor time.

The decay length is part of the type: `ExponentialTimeDecayingFloat64(decay_length)`.
The stored fields form a canonical, order-preserving representation:
`sign Float64`, `signed_unit_time Float64`, and a redundant `decay_length Float64` marker.
For a nonzero curve, `unit_time = anchor_time + decay_length * ln(abs(value_at_anchor))` is
the time at which its magnitude is one. `signed_unit_time` stores `sign * unit_time`.
This layout makes ClickHouse's regular tuple comparison and sorting order match the numeric order
of curves with the same decay length. Zero, including the implicit empty value, is represented as
`(0, 0, decay_length)`.

DateTime and DateTime64 inputs are represented as seconds. The marker is validated against
the type parameter when a value is combined or evaluated, so incompatible decay lengths are not
silently mixed even in paths where ClickHouse treats custom tuple storage as layout-compatible.
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

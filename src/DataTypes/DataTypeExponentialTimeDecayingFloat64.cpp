#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Parsers/ASTLiteral.h>

#include <cmath>
#include <fmt/format.h>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

class SerializationExponentialTimeDecayingFloat64 final : public SerializationWrapper
{
public:
    SerializationExponentialTimeDecayingFloat64(SerializationPtr nested_serialization_, Float64 decay_length_)
        : SerializationWrapper(nested_serialization_)
        , decay_length(decay_length_)
    {
    }

    /// The validation parameter is part of this wrapper and must not be pooled
    /// with a serialization for a different decay length.
    bool supportsPooling() const override { return false; }

    void deserializeBinaryBulk(
        IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override
    {
        const size_t previous_size = column.size();
        nested_serialization->deserializeBinaryBulk(column, istr, rows_offset, limit, avg_value_size_hint);
        validateNewRows(column, previous_size);
    }

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override
    {
        const size_t previous_size = column->size();
        nested_serialization->deserializeBinaryBulkWithMultipleStreams(
            column, rows_offset, limit, settings, state, cache);
        validateNewRows(*column, previous_size);
    }

    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        nested_serialization->deserializeBinary(column, istr, settings);
        validateNewRows(column, previous_size);
    }

    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        nested_serialization->deserializeTextEscaped(column, istr, settings);
        validateNewRows(column, previous_size);
    }

    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        const bool result = nested_serialization->tryDeserializeTextEscaped(column, istr, settings);
        if (result)
            validateNewRows(column, previous_size);
        return result;
    }

    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        nested_serialization->deserializeTextQuoted(column, istr, settings);
        validateNewRows(column, previous_size);
    }

    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        const bool result = nested_serialization->tryDeserializeTextQuoted(column, istr, settings);
        if (result)
            validateNewRows(column, previous_size);
        return result;
    }

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        nested_serialization->deserializeTextCSV(column, istr, settings);
        validateNewRows(column, previous_size);
    }

    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        const bool result = nested_serialization->tryDeserializeTextCSV(column, istr, settings);
        if (result)
            validateNewRows(column, previous_size);
        return result;
    }

    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        nested_serialization->deserializeWholeText(column, istr, settings);
        validateNewRows(column, previous_size);
    }

    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        const bool result = nested_serialization->tryDeserializeWholeText(column, istr, settings);
        if (result)
            validateNewRows(column, previous_size);
        return result;
    }

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        nested_serialization->deserializeTextJSON(column, istr, settings);
        validateNewRows(column, previous_size);
    }

    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        const bool result = nested_serialization->tryDeserializeTextJSON(column, istr, settings);
        if (result)
            validateNewRows(column, previous_size);
        return result;
    }

private:
    void validateNewRows(const IColumn & column, size_t previous_size) const
    {
        if (column.size() <= previous_size)
            return;

        const auto new_rows = column.cut(previous_size, column.size() - previous_size);
        validateExponentialTimeDecayingFloat64Column(
            *new_rows, decay_length, "deserialization");
    }

    const Float64 decay_length;
};

std::pair<DataTypePtr, DataTypeCustomDescPtr> create(Float64 decay_length)
{
    auto storage_type = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeFloat64>()},
        Names{"sign", "signed_unit_time", "decay_length"});

    auto serialization = std::make_shared<SerializationExponentialTimeDecayingFloat64>(
        storage_type->getDefaultSerialization(), decay_length);

    return {
        storage_type,
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomExponentialTimeDecayingFloat64>(decay_length),
            std::move(serialization))};
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
    auto [storage_type, customization] = create(decay_length);
    return DataTypeFactory::instance().getCustom(
        storage_type->getName(), std::move(customization));
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

bool containsExponentialTimeDecayingFloat64(const IDataType & type)
{
    bool contains = isExponentialTimeDecayingFloat64(type);
    if (!contains)
    {
        type.forEachChild([&](const IDataType & child)
        {
            contains |= isExponentialTimeDecayingFloat64(child);
        });
    }
    return contains;
}

bool containsExponentialTimeDecayingFloat64(const DataTypePtr & type)
{
    return type && containsExponentialTimeDecayingFloat64(*type);
}

namespace
{

DataTypePtr removeExponentialTimeDecayingTransparentWrappers(DataTypePtr type)
{
    while (type)
    {
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            type = low_cardinality_type->getDictionaryType();
            continue;
        }

        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            type = nullable_type->getNestedType();
            continue;
        }

        break;
    }

    return type;
}

void assertExponentialTimeDecayingFloat64TypesCompatibleImpl(
    DataTypePtr left_type, DataTypePtr right_type, const String & operation)
{
    left_type = removeExponentialTimeDecayingTransparentWrappers(std::move(left_type));
    right_type = removeExponentialTimeDecayingTransparentWrappers(std::move(right_type));

    const bool left_contains = containsExponentialTimeDecayingFloat64(left_type);
    const bool right_contains = containsExponentialTimeDecayingFloat64(right_type);
    if (!left_contains && !right_contains)
        return;

    if (!left_contains || !right_contains)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "{} cannot combine incompatible types {} and {} containing ExponentialTimeDecayingFloat64",
            operation,
            left_type->getName(),
            right_type->getName());

    const auto left_decay_length = tryGetExponentialTimeDecayingFloat64DecayLength(left_type);
    const auto right_decay_length = tryGetExponentialTimeDecayingFloat64DecayLength(right_type);
    if (left_decay_length || right_decay_length)
    {
        if (!left_decay_length || !right_decay_length)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} cannot combine ExponentialTimeDecayingFloat64 with {}",
                operation,
                left_decay_length ? right_type->getName() : left_type->getName());

        if (*left_decay_length != *right_decay_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} cannot combine ExponentialTimeDecayingFloat64 values with different decay lengths: {} and {}",
                operation,
                *left_decay_length,
                *right_decay_length);
        return;
    }

    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left_type.get()))
    {
        const auto * right_array = typeid_cast<const DataTypeArray *>(right_type.get());
        if (right_array)
        {
            assertExponentialTimeDecayingFloat64TypesCompatibleImpl(
                left_array->getNestedType(), right_array->getNestedType(), operation);
            return;
        }
    }
    else if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_type.get()))
    {
        const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_type.get());
        if (right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                assertExponentialTimeDecayingFloat64TypesCompatibleImpl(
                    left_tuple->getElements()[i], right_tuple->getElements()[i], operation);
            return;
        }
    }
    else if (const auto * left_map = typeid_cast<const DataTypeMap *>(left_type.get()))
    {
        const auto * right_map = typeid_cast<const DataTypeMap *>(right_type.get());
        if (right_map)
        {
            assertExponentialTimeDecayingFloat64TypesCompatibleImpl(
                left_map->getNestedType(), right_map->getNestedType(), operation);
            return;
        }
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "{} cannot combine incompatible types {} and {} containing ExponentialTimeDecayingFloat64",
        operation,
        left_type->getName(),
        right_type->getName());
}

}

void assertExponentialTimeDecayingFloat64TypesCompatible(
    const DataTypePtr & left_type, const DataTypePtr & right_type, const String & operation)
{
    assertExponentialTimeDecayingFloat64TypesCompatibleImpl(left_type, right_type, operation);
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
        if (!isCanonicalExponentialTimeDecayingFloat64Value(sign, signed_unit_time))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecayingFloat64 value in {}: expected canonical sign and signed unit time fields",
                operation);
    }
}

namespace
{

void validateExponentialTimeDecayingFloat64ColumnImpl(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    if (!type || !containsExponentialTimeDecayingFloat64(type))
        return;

    ColumnPtr full_column = column.convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        validateExponentialTimeDecayingFloat64ColumnImpl(
            *full_column, low_cardinality_type->getDictionaryType(), operation);
        return;
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto & nullable_column = assert_cast<const ColumnNullable &>(*full_column);
        validateExponentialTimeDecayingFloat64ColumnImpl(
            nullable_column.getNestedColumn(), nullable_type->getNestedType(), operation);
        return;
    }

    if (const auto decay_length = tryGetExponentialTimeDecayingFloat64DecayLength(type))
    {
        validateExponentialTimeDecayingFloat64Column(*full_column, *decay_length, operation);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto & array_column = assert_cast<const ColumnArray &>(*full_column);
        validateExponentialTimeDecayingFloat64ColumnImpl(
            array_column.getData(), array_type->getNestedType(), operation);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*full_column);
        const auto & element_types = tuple_type->getElements();
        for (size_t i = 0; i < element_types.size(); ++i)
            validateExponentialTimeDecayingFloat64ColumnImpl(
                tuple_column.getColumn(i), element_types[i], operation);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto & map_column = assert_cast<const ColumnMap &>(*full_column);
        validateExponentialTimeDecayingFloat64ColumnImpl(
            map_column.getNestedColumn(), map_type->getNestedType(), operation);
    }
}

}

void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    validateExponentialTimeDecayingFloat64ColumnImpl(column, type, operation);
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

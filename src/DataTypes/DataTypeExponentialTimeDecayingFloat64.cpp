#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnExponentialTimeDecaying.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
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
            "Data type ExponentialTimeDecaying takes exactly one parameter, the decay length");

    const auto * literal = parameters->children[0]->as<ASTLiteral>();
    if (!literal)
        throw Exception(
            ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS,
            "Decay length of data type ExponentialTimeDecaying must be a literal");

    const Float64 decay_length = applyVisitor(FieldVisitorConvertToNumber<Float64>(), literal->value);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Decay length of data type ExponentialTimeDecaying must be finite and positive");

    return decay_length;
}

class SerializationExponentialTimeDecayingFloat64 final : public SerializationWrapper
{
public:
    SerializationExponentialTimeDecayingFloat64(
        SerializationPtr storage_serialization_,
        SerializationPtr logical_serialization_,
        DataTypePtr storage_type_,
        DataTypePtr logical_type_,
        Float64 decay_length_)
        : SerializationWrapper(storage_serialization_)
        , logical_serialization(std::move(logical_serialization_))
        , storage_type(std::move(storage_type_))
        , logical_type(std::move(logical_type_))
        , decay_length(decay_length_)
    {
    }

    bool supportsPooling() const override { return false; }

    MutableColumnPtr wrapColumnForDeserialization(MutableColumnPtr column) const override
    {
        return column;
    }

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override
    {
        auto next_data = SubstreamData(nested_serialization)
                             .withType(data.type ? storage_type : nullptr)
                             .withColumn(
                                 data.column
                                     ? assert_cast<const ColumnExponentialTimeDecaying &>(*data.column).getStoragePtr()
                                     : nullptr)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(data.deserialize_state);
        nested_serialization->enumerateStreams(settings, callback, next_data);
    }

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override
    {
        nested_serialization->serializeBinaryBulkStatePrefix(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            settings,
            state);
    }

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override
    {
        nested_serialization->serializeBinaryBulkWithMultipleStreams(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            offset,
            limit,
            settings,
            state);
    }

    void serializeForHashCalculation(
        const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(column);
        const auto & tuple = decaying.getStorageTuple();
        const Float64 value
            = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData()[row_num];
        const Float64 time
            = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[row_num];

        writeBinaryLittleEndian(
            getExponentialTimeDecayingOrderingKey(
                value, time, decay_length),
            ostr);
    }

    void serializeBinaryBulk(
        const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override
    {
        nested_serialization->serializeBinaryBulk(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            ostr,
            offset,
            limit);
    }

    void serializeBinary(
        const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested_serialization->serializeBinary(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            row_num,
            ostr,
            settings);
    }

    void deserializeBinaryBulk(
        IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override
    {
        const size_t previous_size = column.size();
        auto & decaying = assert_cast<ColumnExponentialTimeDecaying &>(column);
        nested_serialization->deserializeBinaryBulk(
            decaying.getStorageColumn(), istr, limit, avg_value_size_hint);
        decaying.syncOrderingKeyFrom(previous_size);
        validateNewRows(column, previous_size);
    }

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override
    {
        const size_t previous_size = column.size();
        auto & decaying = assert_cast<ColumnExponentialTimeDecaying &>(column);
        nested_serialization->deserializeBinaryBulkWithMultipleStreams(
            decaying.getStorageColumn(), limit, settings, state, cache);
        decaying.syncOrderingKeyFrom(previous_size);
        validateNewRows(column, previous_size);
    }

    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        auto & decaying = assert_cast<ColumnExponentialTimeDecaying &>(column);
        nested_serialization->deserializeBinary(decaying.getStorageColumn(), istr, settings);
        decaying.syncOrderingKeyFrom(previous_size);
        validateNewRows(column, previous_size);
    }

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeTextEscaped(*logical, row_num, ostr, settings);
    }
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextEscaped(logical, istr, settings); });
    }
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextEscaped(logical, istr, settings); });
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeTextQuoted(*logical, row_num, ostr, settings);
    }
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextQuoted(logical, istr, settings); });
    }
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextQuoted(logical, istr, settings); });
    }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeTextCSV(*logical, row_num, ostr, settings);
    }
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextCSV(logical, istr, settings); });
    }
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextCSV(logical, istr, settings); });
    }
    void serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeTextHive(*logical, row_num, ostr, settings);
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeText(*logical, row_num, ostr, settings);
    }
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeWholeText(logical, istr, settings); });
    }
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeWholeText(logical, istr, settings); });
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeTextJSON(*logical, row_num, ostr, settings);
    }
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextJSON(logical, istr, settings); });
    }
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextJSON(logical, istr, settings); });
    }
    void serializeTextJSONPretty(
        const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeTextJSONPretty(*logical, row_num, ostr, settings, indent);
    }
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingFloat64LogicalColumn(column, decay_length);
        logical_serialization->serializeTextXML(*logical, row_num, ostr, settings);
    }

private:
    template <typename Deserialize>
    void deserializeLogical(IColumn & column, Deserialize && deserialize) const
    {
        auto logical = logical_type->createColumn();
        deserialize(*logical);
        auto storage = materializeExponentialTimeDecayingFloat64StorageColumn(*logical, decay_length, "deserialization");
        column.insertRangeFrom(*storage, 0, storage->size());
    }

    template <typename Deserialize>
    bool tryDeserializeLogical(IColumn & column, Deserialize && deserialize) const
    {
        auto logical = logical_type->createColumn();
        if (!deserialize(*logical))
            return false;
        auto storage = materializeExponentialTimeDecayingFloat64StorageColumn(*logical, decay_length, "deserialization");
        column.insertRangeFrom(*storage, 0, storage->size());
        return true;
    }

    void validateNewRows(const IColumn & column, size_t previous_size) const
    {
        if (column.size() <= previous_size)
            return;
        const auto new_rows = column.cut(previous_size, column.size() - previous_size);
        validateExponentialTimeDecayingFloat64Column(*new_rows, "deserialization");
    }

    const SerializationPtr logical_serialization;
    const DataTypePtr storage_type;
    const DataTypePtr logical_type;
    const Float64 decay_length;
};

DataTypePtr createFromParameters(const ASTPtr & parameters)
{
    return std::make_shared<DataTypeExponentialTimeDecayingFloat64>(getDecayLength(parameters));
}

}

DataTypeExponentialTimeDecayingFloat64::DataTypeExponentialTimeDecayingFloat64(
    Float64 decay_length_)
    : decay_length(decay_length_)
    , storage_type(std::make_shared<DataTypeTuple>(
          DataTypes{
              std::make_shared<DataTypeFloat64>(),
              std::make_shared<DataTypeFloat64>()},
          Names{"value_at_anchor", "anchor_time"}))
    , logical_type(std::make_shared<DataTypeTuple>(
          DataTypes{
              std::make_shared<DataTypeFloat64>(),
              std::make_shared<DataTypeFloat64>(),
              std::make_shared<DataTypeFloat64>()},
          Names{"sign", "signed_unit_time", "decay_length"}))
{
}

String DataTypeExponentialTimeDecayingFloat64::doGetName() const
{
    return fmt::format("ExponentialTimeDecaying({})", decay_length);
}

MutableColumnPtr DataTypeExponentialTimeDecayingFloat64::createColumn() const
{
    return ColumnExponentialTimeDecaying::create(storage_type->createColumn(), decay_length);
}

Field DataTypeExponentialTimeDecayingFloat64::getDefault() const
{
    return Tuple{Float64(0), Float64(0)};
}

void DataTypeExponentialTimeDecayingFloat64::insertDefaultInto(IColumn & column) const
{
    column.insert(getDefault());
}

bool DataTypeExponentialTimeDecayingFloat64::equals(const IDataType & rhs) const
{
    const auto * other = typeid_cast<const DataTypeExponentialTimeDecayingFloat64 *>(&rhs);
    return other && decay_length == other->decay_length;
}

bool DataTypeExponentialTimeDecayingFloat64::haveMaximumSizeOfValue() const
{
    return storage_type->haveMaximumSizeOfValue();
}

size_t DataTypeExponentialTimeDecayingFloat64::getMaximumSizeOfValueInMemory() const
{
    return storage_type->getMaximumSizeOfValueInMemory() + sizeof(UInt64);
}

size_t DataTypeExponentialTimeDecayingFloat64::getSizeOfValueInMemory() const
{
    return storage_type->getSizeOfValueInMemory() + sizeof(UInt64);
}

void DataTypeExponentialTimeDecayingFloat64::updateHashImpl(SipHash & hash) const
{
    hash.update(decay_length);
}

SerializationPtr DataTypeExponentialTimeDecayingFloat64::doGetSerialization(const SerializationInfoSettings &) const
{
    return std::make_shared<SerializationExponentialTimeDecayingFloat64>(
        storage_type->getDefaultSerialization(),
        logical_type->getDefaultSerialization(),
        storage_type,
        logical_type,
        decay_length);
}

SerializationPtr DataTypeExponentialTimeDecayingFloat64::getSerialization(const SerializationInfo & info) const
{
    return std::make_shared<SerializationExponentialTimeDecayingFloat64>(
        storage_type->getSerialization(info),
        logical_type->getDefaultSerialization(),
        storage_type,
        logical_type,
        decay_length);
}

MutableSerializationInfoPtr DataTypeExponentialTimeDecayingFloat64::createSerializationInfo(
    const SerializationInfoSettings & settings) const
{
    return storage_type->createSerializationInfo(settings);
}

SerializationInfoPtr DataTypeExponentialTimeDecayingFloat64::getSerializationInfo(
    const IColumn & column, const SerializationInfoSettings & settings) const
{
    const auto & decaying_column = assert_cast<const ColumnExponentialTimeDecaying &>(column);
    return storage_type->getSerializationInfo(decaying_column.getStorageColumn(), settings);
}

DataTypePtr createDataTypeExponentialTimeDecayingFloat64(Float64 decay_length)
{
    return std::make_shared<DataTypeExponentialTimeDecayingFloat64>(decay_length);
}

std::optional<Float64> tryGetExponentialTimeDecayingFloat64DecayLength(const IDataType & type)
{
    if (const auto * decaying_type = typeid_cast<const DataTypeExponentialTimeDecayingFloat64 *>(&type))
        return decaying_type->getDecayLength();

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
    if (isExponentialTimeDecayingFloat64(type))
        return true;

    bool contains = false;
    type.forEachChild([&](const IDataType & child)
    {
        if (!contains)
            contains = containsExponentialTimeDecayingFloat64(child);
    });
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
            "{} cannot combine incompatible types {} and {} containing ExponentialTimeDecaying",
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
                "{} cannot combine ExponentialTimeDecaying with {}",
                operation,
                left_decay_length ? right_type->getName() : left_type->getName());

        if (*left_decay_length != *right_decay_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} cannot combine ExponentialTimeDecaying values with different decay lengths: {} and {}",
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
    else if (const auto * left_variant = typeid_cast<const DataTypeVariant *>(left_type.get()))
    {
        const auto * right_variant = typeid_cast<const DataTypeVariant *>(right_type.get());
        if (right_variant && left_variant->getVariants().size() == right_variant->getVariants().size())
        {
            for (size_t i = 0; i < left_variant->getVariants().size(); ++i)
                assertExponentialTimeDecayingFloat64TypesCompatibleImpl(
                    left_variant->getVariant(i), right_variant->getVariant(i), operation);
            return;
        }
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "{} cannot combine incompatible types {} and {} containing ExponentialTimeDecaying",
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

void assertExponentialTimeDecayingFloat64ConversionTypesCompatible(
    const DataTypePtr & source_type, const DataTypePtr & target_type, const String & operation)
{
    if (!containsExponentialTimeDecayingFloat64(source_type) && !containsExponentialTimeDecayingFloat64(target_type))
        return;

    const auto nested_source_type = removeExponentialTimeDecayingTransparentWrappers(source_type);
    const auto nested_target_type = removeExponentialTimeDecayingTransparentWrappers(target_type);

    /// Variant conversion examines each active alternative separately. Preserve an exact
    /// semantic alternative, but do not accept a layout-compatible plain Tuple instead.
    if (const auto * variant = typeid_cast<const DataTypeVariant *>(nested_target_type.get()))
    {
        for (const auto & alternative : variant->getVariants())
        {
            if (nested_source_type->equals(*alternative))
                return;
        }
    }

    assertExponentialTimeDecayingFloat64TypesCompatible(source_type, target_type, operation);
}

void assertExponentialTimeDecayingFloat64SetKeyTypesCompatible(
    const DataTypePtr & probe_type, const DataTypePtr & set_type)
{
    assertExponentialTimeDecayingFloat64ConversionTypesCompatible(probe_type, set_type, "IN");
}

ColumnPtr materializeExponentialTimeDecayingFloat64LogicalColumn(
    const IColumn & storage_column, Float64 decay_length)
{
    ColumnPtr full = storage_column.convertToFullColumnIfConst();
    const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*full);
    const auto & tuple = decaying.getStorageTuple();
    const auto & values = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & times = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();

    auto signs = ColumnFloat64::create();
    auto signed_unit_times = ColumnFloat64::create();
    auto decay_lengths = ColumnFloat64::create(tuple.size(), decay_length);
    signs->reserve(tuple.size());
    signed_unit_times->reserve(tuple.size());

    for (size_t row = 0; row < tuple.size(); ++row)
    {
        const Float64 value = values[row];
        if (value == 0)
        {
            signs->insertValue(0);
            signed_unit_times->insertValue(0);
            continue;
        }

        const Float64 sign = std::copysign(1.0, value);
        const Float64 unit_timestamp
            = getExponentialTimeDecayingUnitTimestamp(value, times[row], decay_length);
        signs->insertValue(sign);
        signed_unit_times->insertValue(sign * unit_timestamp);
    }

    return ColumnTuple::create(
        Columns{std::move(signs), std::move(signed_unit_times), std::move(decay_lengths)});
}

ColumnPtr materializeExponentialTimeDecayingFloat64StorageColumn(
    const IColumn & logical_column, Float64 decay_length, const String & operation)
{
    ColumnPtr full = logical_column.convertToFullColumnIfConst();
    const auto & tuple = assert_cast<const ColumnTuple &>(*full);
    if (tuple.tupleSize() != 3)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Malformed ExponentialTimeDecaying value in {}: expected three logical fields",
            operation);

    const auto & signs = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & signed_unit_times = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();
    const auto & decay_lengths = assert_cast<const ColumnFloat64 &>(tuple.getColumn(2)).getData();

    auto values = ColumnFloat64::create();
    auto times = ColumnFloat64::create();
    values->reserve(tuple.size());
    times->reserve(tuple.size());

    for (size_t row = 0; row < tuple.size(); ++row)
    {
        if (!std::isfinite(decay_lengths[row]) || decay_lengths[row] != decay_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecaying value in {}: supplied decay length {} does not match type decay length {}",
                operation,
                decay_lengths[row],
                decay_length);

        const Float64 sign = signs[row];
        const Float64 signed_unit_time = signed_unit_times[row];
        if (sign == 0)
        {
            if (signed_unit_time != 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Malformed ExponentialTimeDecaying value in {}: zero must have zero unit timestamp",
                    operation);
            values->insertValue(0);
            times->insertValue(0);
            continue;
        }

        if ((sign != -1 && sign != 1) || !std::isfinite(signed_unit_time))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecaying value in {}: expected canonical sign and signed unit timestamp",
                operation);

        values->insertValue(sign);
        times->insertValue(sign * signed_unit_time);
    }

    auto physical = ColumnTuple::create(Columns{std::move(values), std::move(times)});
    return ColumnExponentialTimeDecaying::create(physical->assumeMutable(), decay_length);
}

void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, const String & operation)
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

    const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*nested_column);
    const auto & tuple = decaying.getStorageTuple();
    if (tuple.tupleSize() != 2)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Malformed ExponentialTimeDecaying value in {}: expected direct value and anchor payload",
            operation);

    const auto & values = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & times = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();
    const auto & ordering_key
        = assert_cast<const ColumnUInt64 &>(decaying.getOrderingKeyColumn()).getData();

    for (size_t row = 0; row < tuple.size(); ++row)
    {
        if (nullable && nullable->isNullAt(row))
            continue;

        const auto normalized
            = normalizeExponentialTimeDecayingFloat64(values[row], times[row], decaying.getDecayLength());
        if (!std::isfinite(values[row])
            || !std::isfinite(times[row])
            || normalized.value_at_anchor != values[row]
            || normalized.anchor_time != times[row]
            || normalized.ordering_key != ordering_key[row])
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecaying value in {}: direct payload or derived ordering key is invalid",
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

    if (isExponentialTimeDecayingFloat64(type))
    {
        validateExponentialTimeDecayingFloat64Column(*full_column, operation);
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
        return;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        const auto & variant_column = assert_cast<const ColumnVariant &>(*full_column);
        for (size_t i = 0; i < variant_type->getVariants().size(); ++i)
            validateExponentialTimeDecayingFloat64ColumnImpl(
                variant_column.getVariantByGlobalDiscriminator(i), variant_type->getVariant(i), operation);
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
    const Documentation documentation{
        .description = R"(
Represents a finite exponentially time-decaying value.

The decay length is part of the logical type: `ExponentialTimeDecaying(decay_length)` and is not
stored per row. The persisted payload contains the authoritative
`(value_at_anchor, anchor_time)` pair. Arithmetic uses that direct payload.

For ordering, equality, hashing, arena serialization, primary-key marks, and `minmax` indexes, the
type derives one 8-byte `UInt64` key with `shiftOneBitAndSign(unit_timestamp)`. The sign divides
the key space around zero and one low-order bit of the sortable `Float64` unit timestamp is
discarded. Neighboring curves can therefore share an ordering key; when they do, the type treats
them as equal for ordering and hashing.

For a nonzero curve, `unit_timestamp = anchor_time + decay_length * ln(abs(value_at_anchor))` is the
time at which its magnitude is one. SQL/text presentation exposes
`(sign, signed_unit_time, decay_length)`; the decay length is synthesized from the type rather
than stored per row.

`PARTITION BY` is not supported for this experimental type. DateTime and DateTime64 inputs are
represented as seconds. Values with different decay lengths are different logical types and cannot
be mixed.
)",
        .syntax = "ExponentialTimeDecaying(decay_length)",
        .examples = {},
        .related = {"SimpleAggregateFunction"},
    };

    factory.registerDataType(
        "ExponentialTimeDecaying",
        createFromParameters,
        DataTypeFactory::Case::Sensitive,
        documentation);

}
}

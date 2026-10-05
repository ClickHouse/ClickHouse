#include <DataTypes/DataTypeExponentialTimeDecaying.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
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
#include <boost/multiprecision/cpp_bin_float.hpp>

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

using ExtendedTimeDecayFloat = boost::multiprecision::cpp_bin_float_quad;

ExponentialTimeDecayingCanonicalDirectValue normalizeExponentialTimeDecaying128Direct(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (value == 0)
        return {0, 0};

    const bool negative = std::signbit(value);
    const ExtendedTimeDecayFloat unit_timestamp
        = ExtendedTimeDecayFloat(time)
        + ExtendedTimeDecayFloat(decay_length) * log(ExtendedTimeDecayFloat(std::abs(value)));

    Float64 anchor = static_cast<Float64>(unit_timestamp);
    if (ExtendedTimeDecayFloat(anchor) > unit_timestamp)
        anchor = std::nextafter(anchor, -std::numeric_limits<Float64>::infinity());

    auto magnitude_at = [&](Float64 candidate)
    {
        return static_cast<Float64>(
            exp((unit_timestamp - ExtendedTimeDecayFloat(candidate))
                / ExtendedTimeDecayFloat(decay_length)));
    };

    Float64 magnitude = magnitude_at(anchor);
    if (!std::isfinite(magnitude) || magnitude == 0)
    {
        anchor = std::nextafter(anchor, std::numeric_limits<Float64>::infinity());
        magnitude = magnitude_at(anchor);
    }

    if (!std::isfinite(anchor) || !std::isfinite(magnitude) || magnitude == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ExponentialTimeDecaying128 value cannot be represented by a finite canonical anchor");

    return {negative ? -magnitude : magnitude, anchor};
}

UInt128 packExponentialTimeDecaying128Key(
    Float64 value_at_anchor, Float64 anchor_time)
{
    constexpr UInt128 midpoint = UInt128(1) << 127;
    constexpr UInt64 magnitude_mask = (UInt64(1) << 63) - 1;

    if (value_at_anchor == 0)
        return midpoint;

    const UInt64 sortable_anchor = getExponentialTimeDecayingSortableFloatKey(anchor_time);
    const UInt64 sortable_magnitude
        = getExponentialTimeDecayingSortableFloatKey(std::abs(value_at_anchor));
    const UInt128 timestamp_key
        = (UInt128(sortable_anchor) << 63)
        | UInt128(sortable_magnitude & magnitude_mask);

    if (std::signbit(value_at_anchor))
        return midpoint - 1 - timestamp_key;
    return midpoint + timestamp_key;
}

Float64 getDecayLength(const ASTPtr & parameters, const char * type_name)
{
    if (!parameters || parameters->children.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Data type {} takes exactly one parameter, the decay length",
            type_name);

    const auto * literal = parameters->children[0]->as<ASTLiteral>();
    if (!literal)
        throw Exception(
            ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS,
            "Decay length of data type {} must be a literal",
            type_name);

    const Float64 decay_length = applyVisitor(FieldVisitorConvertToNumber<Float64>(), literal->value);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Decay length of data type {} must be finite and positive",
            type_name);

    return decay_length;
}

class SerializationExponentialTimeDecaying final : public SerializationWrapper
{
public:
    SerializationExponentialTimeDecaying(
        SerializationPtr storage_serialization_,
        SerializationPtr logical_serialization_,
        DataTypePtr storage_type_,
        DataTypePtr logical_type_,
        Float64 decay_length_,
        ExponentialTimeDecayingKeyWidth key_width_)
        : SerializationWrapper(storage_serialization_)
        , logical_serialization(std::move(logical_serialization_))
        , storage_type(std::move(storage_type_))
        , logical_type(std::move(logical_type_))
        , decay_length(decay_length_)
        , key_width(key_width_)
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
        if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        {
            const auto & keys
                = assert_cast<const ColumnUInt64 &>(decaying.getOrderingKeyColumn()).getData();
            writeBinaryLittleEndian(keys[row_num], ostr);
        }
        else
        {
            const auto & keys
                = assert_cast<const ColumnUInt128 &>(decaying.getOrderingKeyColumn()).getData();
            writeBinaryLittleEndian(keys[row_num], ostr);
        }
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
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
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
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
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
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
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
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextHive(*logical, row_num, ostr, settings);
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
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
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
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
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextJSONPretty(*logical, row_num, ostr, settings, indent);
    }
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextXML(*logical, row_num, ostr, settings);
    }

private:
    template <typename Deserialize>
    void deserializeLogical(IColumn & column, Deserialize && deserialize) const
    {
        auto logical = logical_type->createColumn();
        deserialize(*logical);
        auto storage = materializeExponentialTimeDecayingStorageColumn(*logical, decay_length, "deserialization", key_width);
        column.insertRangeFrom(*storage, 0, storage->size());
    }

    template <typename Deserialize>
    bool tryDeserializeLogical(IColumn & column, Deserialize && deserialize) const
    {
        auto logical = logical_type->createColumn();
        if (!deserialize(*logical))
            return false;
        auto storage = materializeExponentialTimeDecayingStorageColumn(*logical, decay_length, "deserialization", key_width);
        column.insertRangeFrom(*storage, 0, storage->size());
        return true;
    }

    void validateNewRows(const IColumn & column, size_t previous_size) const
    {
        if (column.size() <= previous_size)
            return;
        const auto new_rows = column.cut(previous_size, column.size() - previous_size);
        validateExponentialTimeDecayingColumn(*new_rows, "deserialization");
    }

    const SerializationPtr logical_serialization;
    const DataTypePtr storage_type;
    const DataTypePtr logical_type;
    const Float64 decay_length;
    const ExponentialTimeDecayingKeyWidth key_width;
};

DataTypePtr createFromParameters64(const ASTPtr & parameters)
{
    return std::make_shared<DataTypeExponentialTimeDecaying>(
        getDecayLength(parameters, "ExponentialTimeDecaying64"),
        ExponentialTimeDecayingKeyWidth::Bits64);
}

DataTypePtr createFromParameters128(const ASTPtr & parameters)
{
    return std::make_shared<DataTypeExponentialTimeDecaying>(
        getDecayLength(parameters, "ExponentialTimeDecaying128"),
        ExponentialTimeDecayingKeyWidth::Bits128);
}

}


UInt128 getExponentialTimeDecayingOrderingKey128(
    Float64 value, Float64 time, Float64 decay_length)
{
    const auto direct = normalizeExponentialTimeDecaying128Direct(value, time, decay_length);
    return packExponentialTimeDecaying128Key(direct.value_at_anchor, direct.anchor_time);
}

ExponentialTimeDecayingCanonicalDirectValue
getExponentialTimeDecayingCanonicalDirectValue(UInt128 ordering_key)
{
    constexpr UInt128 midpoint = UInt128(1) << 127;
    constexpr UInt128 residual_mask = (UInt128(1) << 63) - 1;

    if (ordering_key == midpoint)
        return {0, 0};

    const bool negative = ordering_key < midpoint;
    const UInt128 timestamp_key = negative
        ? midpoint - 1 - ordering_key
        : ordering_key - midpoint;

    const UInt64 sortable_anchor = static_cast<UInt64>(timestamp_key >> 63);
    const UInt64 sortable_magnitude
        = (UInt64(1) << 63) | static_cast<UInt64>(timestamp_key & residual_mask);

    const Float64 anchor_time
        = getExponentialTimeDecayingFloatFromSortableKey(sortable_anchor);
    const Float64 magnitude
        = getExponentialTimeDecayingFloatFromSortableKey(sortable_magnitude);

    if (!std::isfinite(anchor_time) || !std::isfinite(magnitude) || magnitude <= 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Serialized ExponentialTimeDecaying128 ordering key is invalid");

    return {negative ? -magnitude : magnitude, anchor_time};
}

bool isFiniteExponentialTimeDecayingCurve(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (!std::isfinite(value)
        || !std::isfinite(time)
        || !std::isfinite(decay_length)
        || decay_length <= 0)
        return false;

    return value == 0
        || std::isfinite(getExponentialTimeDecayingUnitTimestamp(value, time, decay_length));
}

ExponentialTimeDecayingValue normalizeExponentialTimeDecaying(
    Float64 value,
    Float64 time,
    Float64 decay_length,
    ExponentialTimeDecayingKeyWidth key_width)
{
    if (value == 0)
    {
        const UInt128 ordering_key = key_width == ExponentialTimeDecayingKeyWidth::Bits64
            ? UInt128(shiftOneBitAndSign(0, 0))
            : (UInt128(1) << 127);
        return {ordering_key, 0, 0};
    }

    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        return {
            UInt128(getExponentialTimeDecayingOrderingKey(value, time, decay_length)),
            value,
            time};

    const auto direct = normalizeExponentialTimeDecaying128Direct(value, time, decay_length);
    return {
        packExponentialTimeDecaying128Key(direct.value_at_anchor, direct.anchor_time),
        direct.value_at_anchor,
        direct.anchor_time};
}

DataTypeExponentialTimeDecaying::DataTypeExponentialTimeDecaying(
    Float64 decay_length_,
    ExponentialTimeDecayingKeyWidth key_width_)
    : decay_length(decay_length_)
    , key_width(key_width_)
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
          Names{"value", "timestamp", "decay_length"}))
{
}

String DataTypeExponentialTimeDecaying::doGetName() const
{
    return fmt::format("{}({})", getExponentialTimeDecayingTypeName(key_width), decay_length);
}

MutableColumnPtr DataTypeExponentialTimeDecaying::createColumn() const
{
    return ColumnExponentialTimeDecaying::create(storage_type->createColumn(), decay_length, key_width);
}

Field DataTypeExponentialTimeDecaying::getDefault() const
{
    return Tuple{Float64(0), Float64(0)};
}

void DataTypeExponentialTimeDecaying::insertDefaultInto(IColumn & column) const
{
    column.insert(getDefault());
}

bool DataTypeExponentialTimeDecaying::equals(const IDataType & rhs) const
{
    const auto * other = typeid_cast<const DataTypeExponentialTimeDecaying *>(&rhs);
    return other
        && decay_length == other->decay_length
        && key_width == other->key_width;
}

bool DataTypeExponentialTimeDecaying::haveMaximumSizeOfValue() const
{
    return storage_type->haveMaximumSizeOfValue();
}

size_t DataTypeExponentialTimeDecaying::getMaximumSizeOfValueInMemory() const
{
    return storage_type->getMaximumSizeOfValueInMemory()
        + (key_width == ExponentialTimeDecayingKeyWidth::Bits64 ? sizeof(UInt64) : sizeof(UInt128));
}

size_t DataTypeExponentialTimeDecaying::getSizeOfValueInMemory() const
{
    return storage_type->getSizeOfValueInMemory()
        + (key_width == ExponentialTimeDecayingKeyWidth::Bits64 ? sizeof(UInt64) : sizeof(UInt128));
}

void DataTypeExponentialTimeDecaying::updateHashImpl(SipHash & hash) const
{
    hash.update(decay_length);
    hash.update(static_cast<UInt8>(key_width));
}

SerializationPtr DataTypeExponentialTimeDecaying::doGetSerialization(const SerializationInfoSettings &) const
{
    return std::make_shared<SerializationExponentialTimeDecaying>(
        storage_type->getDefaultSerialization(),
        logical_type->getDefaultSerialization(),
        storage_type,
        logical_type,
        decay_length,
        key_width);
}

SerializationPtr DataTypeExponentialTimeDecaying::getSerialization(const SerializationInfo & info) const
{
    return std::make_shared<SerializationExponentialTimeDecaying>(
        storage_type->getSerialization(info),
        logical_type->getDefaultSerialization(),
        storage_type,
        logical_type,
        decay_length,
        key_width);
}

MutableSerializationInfoPtr DataTypeExponentialTimeDecaying::createSerializationInfo(
    const SerializationInfoSettings & settings) const
{
    return storage_type->createSerializationInfo(settings);
}

SerializationInfoPtr DataTypeExponentialTimeDecaying::getSerializationInfo(
    const IColumn & column, const SerializationInfoSettings & settings) const
{
    if (const auto * column_const = checkAndGetColumn<ColumnConst>(&column))
        return getSerializationInfo(column_const->getDataColumn(), settings);

    const auto & decaying_column = assert_cast<const ColumnExponentialTimeDecaying &>(column);
    return storage_type->getSerializationInfo(decaying_column.getStorageColumn(), settings);
}

DataTypePtr createDataTypeExponentialTimeDecaying(
    Float64 decay_length,
    ExponentialTimeDecayingKeyWidth key_width)
{
    return std::make_shared<DataTypeExponentialTimeDecaying>(decay_length, key_width);
}

std::optional<Float64> tryGetExponentialTimeDecayingDecayLength(const IDataType & type)
{
    if (const auto * decaying_type = typeid_cast<const DataTypeExponentialTimeDecaying *>(&type))
        return decaying_type->getDecayLength();

    return std::nullopt;
}

std::optional<Float64> tryGetExponentialTimeDecayingDecayLength(const DataTypePtr & type)
{
    return type ? tryGetExponentialTimeDecayingDecayLength(*type) : std::nullopt;
}

std::optional<ExponentialTimeDecayingKeyWidth> tryGetExponentialTimeDecayingKeyWidth(const IDataType & type)
{
    if (const auto * decaying_type = typeid_cast<const DataTypeExponentialTimeDecaying *>(&type))
        return decaying_type->getKeyWidth();

    return std::nullopt;
}

std::optional<ExponentialTimeDecayingKeyWidth> tryGetExponentialTimeDecayingKeyWidth(const DataTypePtr & type)
{
    return type ? tryGetExponentialTimeDecayingKeyWidth(*type) : std::nullopt;
}

bool isExponentialTimeDecaying(const IDataType & type)
{
    return tryGetExponentialTimeDecayingDecayLength(type).has_value();
}

bool isExponentialTimeDecaying(const DataTypePtr & type)
{
    return tryGetExponentialTimeDecayingDecayLength(type).has_value();
}

bool containsExponentialTimeDecaying(const IDataType & type)
{
    if (isExponentialTimeDecaying(type))
        return true;

    bool contains = false;
    type.forEachChild([&](const IDataType & child)
    {
        if (!contains)
            contains = isExponentialTimeDecaying(child);
    });
    return contains;
}

bool containsExponentialTimeDecaying(const DataTypePtr & type)
{
    return type && containsExponentialTimeDecaying(*type);
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

void assertExponentialTimeDecayingTypesCompatibleImpl(
    DataTypePtr left_type, DataTypePtr right_type, const String & operation)
{
    left_type = removeExponentialTimeDecayingTransparentWrappers(std::move(left_type));
    right_type = removeExponentialTimeDecayingTransparentWrappers(std::move(right_type));

    const bool left_contains = containsExponentialTimeDecaying(left_type);
    const bool right_contains = containsExponentialTimeDecaying(right_type);
    if (!left_contains && !right_contains)
        return;

    if (!left_contains || !right_contains)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "{} cannot combine incompatible types {} and {} containing ExponentialTimeDecaying",
            operation,
            left_type->getName(),
            right_type->getName());

    const auto left_decay_length = tryGetExponentialTimeDecayingDecayLength(left_type);
    const auto right_decay_length = tryGetExponentialTimeDecayingDecayLength(right_type);
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
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} cannot combine ExponentialTimeDecaying values with different decay lengths: {} and {}",
                operation,
                *left_decay_length,
                *right_decay_length);

        if (tryGetExponentialTimeDecayingKeyWidth(left_type)
            != tryGetExponentialTimeDecayingKeyWidth(right_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} cannot implicitly combine {} and {}",
                operation,
                left_type->getName(),
                right_type->getName());
        return;
    }

    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left_type.get()))
    {
        const auto * right_array = typeid_cast<const DataTypeArray *>(right_type.get());
        if (right_array)
        {
            assertExponentialTimeDecayingTypesCompatibleImpl(
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
                assertExponentialTimeDecayingTypesCompatibleImpl(
                    left_tuple->getElements()[i], right_tuple->getElements()[i], operation);
            return;
        }
    }
    else if (const auto * left_map = typeid_cast<const DataTypeMap *>(left_type.get()))
    {
        const auto * right_map = typeid_cast<const DataTypeMap *>(right_type.get());
        if (right_map)
        {
            assertExponentialTimeDecayingTypesCompatibleImpl(
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
                assertExponentialTimeDecayingTypesCompatibleImpl(
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

void assertExponentialTimeDecayingTypesCompatible(
    const DataTypePtr & left_type, const DataTypePtr & right_type, const String & operation)
{
    assertExponentialTimeDecayingTypesCompatibleImpl(left_type, right_type, operation);
}

void assertExponentialTimeDecayingConversionTypesCompatible(
    const DataTypePtr & source_type, const DataTypePtr & target_type, const String & operation)
{
    if (!containsExponentialTimeDecaying(source_type) && !containsExponentialTimeDecaying(target_type))
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

    assertExponentialTimeDecayingTypesCompatible(source_type, target_type, operation);
}

void assertExponentialTimeDecayingSetKeyTypesCompatible(
    const DataTypePtr & probe_type, const DataTypePtr & set_type)
{
    assertExponentialTimeDecayingConversionTypesCompatible(probe_type, set_type, "IN");
}

ColumnPtr materializeExponentialTimeDecayingLogicalColumn(
    const IColumn & storage_column, Float64 decay_length)
{
    ColumnPtr full = storage_column.convertToFullColumnIfConst();
    const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*full);
    const auto & tuple = decaying.getStorageTuple();

    auto decay_lengths = ColumnFloat64::create(tuple.size(), decay_length);
    return ColumnTuple::create(
        Columns{
            tuple.getColumnPtr(0),
            tuple.getColumnPtr(1),
            std::move(decay_lengths)});
}

ColumnPtr materializeExponentialTimeDecayingStorageColumn(
    const IColumn & logical_column,
    Float64 decay_length,
    const String & operation,
    ExponentialTimeDecayingKeyWidth key_width)
{
    ColumnPtr full = logical_column.convertToFullColumnIfConst();
    const auto & tuple = assert_cast<const ColumnTuple &>(*full);
    if (tuple.tupleSize() != 2 && tuple.tupleSize() != 3)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Malformed ExponentialTimeDecaying value in {}: expected raw (value, timestamp[, decay_length]) tuple",
            operation);

    const auto & values = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & times = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();

    const ColumnFloat64 * decay_lengths = nullptr;
    if (tuple.tupleSize() == 3)
        decay_lengths = &assert_cast<const ColumnFloat64 &>(tuple.getColumn(2));

    auto storage_values = ColumnFloat64::create();
    auto storage_times = ColumnFloat64::create();
    storage_values->reserve(tuple.size());
    storage_times->reserve(tuple.size());

    for (size_t row = 0; row < tuple.size(); ++row)
    {
        if (decay_lengths)
        {
            const Float64 supplied_decay_length = decay_lengths->getData()[row];
            if (!std::isfinite(supplied_decay_length) || supplied_decay_length != decay_length)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Malformed ExponentialTimeDecaying value in {}: supplied decay length {} does not match type decay length {}",
                    operation,
                    supplied_decay_length,
                    decay_length);
        }

        const Float64 value = values[row];
        const Float64 time = times[row];
        if (!isFiniteExponentialTimeDecayingCurve(value, time, decay_length))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecaying value in {}: value and timestamp must define a finite decay curve",
                operation);

        const auto normalized = normalizeExponentialTimeDecaying(
            value, time, decay_length, key_width);
        storage_values->insertValue(normalized.value_at_anchor);
        storage_times->insertValue(normalized.anchor_time);
    }

    auto physical = ColumnTuple::create(
        Columns{std::move(storage_values), std::move(storage_times)});
    return ColumnExponentialTimeDecaying::create(
        physical->assumeMutable(), decay_length, key_width);
}

void validateExponentialTimeDecayingColumn(
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
    for (size_t row = 0; row < tuple.size(); ++row)
    {
        if (nullable && nullable->isNullAt(row))
            continue;

        if (!isFiniteExponentialTimeDecayingCurve(
                values[row], times[row], decaying.getDecayLength()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecaying value in {}: direct payload is invalid",
                operation);

        const auto normalized = normalizeExponentialTimeDecaying(
            values[row],
            times[row],
            decaying.getDecayLength(),
            decaying.getKeyWidth());

        if (decaying.getKeyWidth() == ExponentialTimeDecayingKeyWidth::Bits64)
        {
            const auto & keys
                = assert_cast<const ColumnUInt64 &>(decaying.getOrderingKeyColumn()).getData();
            if (static_cast<UInt64>(normalized.ordering_key) != keys[row])
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Malformed ExponentialTimeDecaying64 value in {}: derived ordering key is invalid",
                    operation);
        }
        else
        {
            const auto & keys
                = assert_cast<const ColumnUInt128 &>(decaying.getOrderingKeyColumn()).getData();
            if (normalized.ordering_key != keys[row])
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Malformed ExponentialTimeDecaying128 value in {}: derived ordering key is invalid",
                    operation);
        }
    }
}

namespace
{

void validateExponentialTimeDecayingColumnImpl(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    if (!type || !containsExponentialTimeDecaying(type))
        return;

    ColumnPtr full_column = column.convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        validateExponentialTimeDecayingColumnImpl(
            *full_column, low_cardinality_type->getDictionaryType(), operation);
        return;
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto & nullable_column = assert_cast<const ColumnNullable &>(*full_column);
        validateExponentialTimeDecayingColumnImpl(
            nullable_column.getNestedColumn(), nullable_type->getNestedType(), operation);
        return;
    }

    if (isExponentialTimeDecaying(type))
    {
        validateExponentialTimeDecayingColumn(*full_column, operation);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto & array_column = assert_cast<const ColumnArray &>(*full_column);
        validateExponentialTimeDecayingColumnImpl(
            array_column.getData(), array_type->getNestedType(), operation);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*full_column);
        const auto & element_types = tuple_type->getElements();
        for (size_t i = 0; i < element_types.size(); ++i)
            validateExponentialTimeDecayingColumnImpl(
                tuple_column.getColumn(i), element_types[i], operation);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto & map_column = assert_cast<const ColumnMap &>(*full_column);
        validateExponentialTimeDecayingColumnImpl(
            map_column.getNestedColumn(), map_type->getNestedType(), operation);
        return;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        const auto & variant_column = assert_cast<const ColumnVariant &>(*full_column);
        for (size_t i = 0; i < variant_type->getVariants().size(); ++i)
            validateExponentialTimeDecayingColumnImpl(
                variant_column.getVariantByGlobalDiscriminator(i), variant_type->getVariant(i), operation);
    }
}

}

void validateExponentialTimeDecayingColumn(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    validateExponentialTimeDecayingColumnImpl(column, type, operation);
}

void registerDataTypeExponentialTimeDecaying(DataTypeFactory & factory)
{
    const Documentation documentation64{
        .description = R"(
Represents an exponentially time-decaying curve whose complete logical identity is a compact
`UInt64` ordering key. Curves mapping to the same key compare equal and have the same hash.
The direct payload is `(value_at_anchor, anchor_time)`, and `decay_length` is part of the type.
)",
        .syntax = "ExponentialTimeDecaying64(decay_length)",
        .examples = {},
        .related = {"SimpleAggregateFunction", "ExponentialTimeDecaying128"},
    };

    const Documentation documentation128{
        .description = R"(
Represents an exponentially time-decaying curve whose complete logical identity is a `UInt128`
ordering key. The wider key uses a deterministic extended-precision unit timestamp and retains
additional residual timestamp precision compared with `ExponentialTimeDecaying64`.
The direct payload is `(value_at_anchor, anchor_time)`, and `decay_length` is part of the type.
)",
        .syntax = "ExponentialTimeDecaying128(decay_length)",
        .examples = {},
        .related = {"SimpleAggregateFunction", "ExponentialTimeDecaying64"},
    };

    factory.registerDataType(
        "ExponentialTimeDecaying64",
        createFromParameters64,
        DataTypeFactory::Case::Sensitive,
        documentation64);

    factory.registerDataType(
        "ExponentialTimeDecaying128",
        createFromParameters128,
        DataTypeFactory::Case::Sensitive,
        documentation128);
}

}

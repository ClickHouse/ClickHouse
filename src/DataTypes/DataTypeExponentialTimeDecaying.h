#pragma once

#include <DataTypes/IDataType.h>

#include <bit>
#include <cmath>
#include <optional>
#include <limits>

namespace DB
{

class DataTypeFactory;

enum class ExponentialTimeDecayingKeyWidth : UInt8
{
    Bits64 = 64,
    Bits128 = 128,
};

enum class ExponentialTimeDecayingMode : UInt8
{
    Compact,
    Direct,
};

ExponentialTimeDecayingMode parseExponentialTimeDecayingMode(const String & name);

inline const char * getExponentialTimeDecayingModeName(ExponentialTimeDecayingMode mode)
{
    return mode == ExponentialTimeDecayingMode::Compact ? "compact" : "direct";
}

inline const char * getExponentialTimeDecayingTypeName(ExponentialTimeDecayingKeyWidth width)
{
    return width == ExponentialTimeDecayingKeyWidth::Bits64
        ? "ExponentialTimeDecaying64"
        : "ExponentialTimeDecaying128";
}

inline Float64 getExponentialTimeDecayingUnitTimestamp(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (value == 0)
        return 0;

    return time + decay_length * std::log(std::abs(value));
}

inline UInt64 getExponentialTimeDecayingSortableFloatKey(Float64 value)
{
    UInt64 bits = std::bit_cast<UInt64>(value == 0.0 ? 0.0 : value);
    return bits & (UInt64(1) << 63) ? ~bits : bits | (UInt64(1) << 63);
}

inline Float64 getExponentialTimeDecayingFloatFromSortableKey(UInt64 key)
{
    const UInt64 bits
        = key & (UInt64(1) << 63)
        ? key & ~(UInt64(1) << 63)
        : ~key;
    return std::bit_cast<Float64>(bits);
}

/// Compact 64-bit ordered identity. One sortable timestamp bit is discarded to
/// make room for the curve-sign domain.
inline UInt64 shiftOneBitAndSign(
    Float64 unit_timestamp, Float64 value_at_anchor)
{
    constexpr UInt64 midpoint = UInt64(1) << 63;

    if (value_at_anchor == 0)
        return midpoint;

    const UInt64 unit_key = getExponentialTimeDecayingSortableFloatKey(unit_timestamp) >> 1;
    if (std::signbit(value_at_anchor))
        return midpoint - 1 - unit_key;

    return midpoint + unit_key;
}

inline UInt64 getExponentialTimeDecayingOrderingKey(
    Float64 value, Float64 time, Float64 decay_length)
{
    Float64 unit_timestamp = getExponentialTimeDecayingUnitTimestamp(value, time, decay_length);
    if (!std::isfinite(unit_timestamp))
        unit_timestamp = std::copysign(std::numeric_limits<Float64>::max(), unit_timestamp);

    return shiftOneBitAndSign(unit_timestamp, value);
}

struct ExponentialTimeDecayingCanonicalDirectValue
{
    Float64 value_at_anchor;
    Float64 anchor_time;
};

inline ExponentialTimeDecayingCanonicalDirectValue
getExponentialTimeDecayingCanonicalDirectValue(UInt64 ordering_key)
{
    constexpr UInt64 midpoint = UInt64(1) << 63;

    if (ordering_key == midpoint)
        return {0, 0};

    const bool negative = ordering_key < midpoint;
    const UInt64 distance = negative
        ? midpoint - 1 - ordering_key
        : ordering_key - midpoint;

    UInt64 sortable = distance << 1;
    Float64 unit_timestamp = getExponentialTimeDecayingFloatFromSortableKey(sortable);
    if (!std::isfinite(unit_timestamp))
    {
        if (negative)
            ++sortable;
        else
            --sortable;
        unit_timestamp = getExponentialTimeDecayingFloatFromSortableKey(sortable);
    }

    return {negative ? -1.0 : 1.0, unit_timestamp};
}

UInt128 getExponentialTimeDecayingOrderingKey128(
    Float64 value, Float64 time, Float64 decay_length);

ExponentialTimeDecayingCanonicalDirectValue
getExponentialTimeDecayingCanonicalDirectValue(UInt128 ordering_key);

int compareExponentialTimeDecayingOrderingKeyDistance(
    UInt128 lhs_ordering_key,
    UInt128 rhs_ordering_key,
    ExponentialTimeDecayingKeyWidth key_width,
    Float64 decay_length,
    Float64 max_distance_in_decay_lengths);

struct ExponentialTimeDecayingValue
{
    UInt128 ordering_key;
    Float64 value_at_anchor;
    Float64 anchor_time;
};

bool isFiniteExponentialTimeDecayingCurve(
    Float64 value, Float64 time, Float64 decay_length);

ExponentialTimeDecayingValue normalizeExponentialTimeDecaying(
    Float64 value,
    Float64 time,
    Float64 decay_length,
    ExponentialTimeDecayingKeyWidth key_width = ExponentialTimeDecayingKeyWidth::Bits64);

class DataTypeExponentialTimeDecaying final : public IDataType
{
public:
    explicit DataTypeExponentialTimeDecaying(
        Float64 decay_length_,
        ExponentialTimeDecayingKeyWidth key_width_ = ExponentialTimeDecayingKeyWidth::Bits64,
        ExponentialTimeDecayingMode mode_ = ExponentialTimeDecayingMode::Compact);

    TypeIndex getTypeId() const override { return TypeIndex::ExponentialTimeDecaying; }
    TypeIndex getColumnType() const override { return TypeIndex::ExponentialTimeDecaying; }
    String doGetName() const override;
    const char * getFamilyName() const override { return getExponentialTimeDecayingTypeName(key_width); }

    MutableColumnPtr createColumn() const override;
    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;
    bool isDefaultInsertTrivial() const override { return false; }

    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool canBeInsideNullable() const override { return true; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }
    bool isComparable() const override { return true; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool haveMaximumSizeOfValue() const override;
    size_t getMaximumSizeOfValueInMemory() const override;
    size_t getSizeOfValueInMemory() const override;
    void updateHashImpl(SipHash & hash) const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;
    SerializationPtr getSerialization(const SerializationInfo & info) const override;
    MutableSerializationInfoPtr createSerializationInfo(const SerializationInfoSettings & settings) const override;
    SerializationInfoPtr getSerializationInfo(const IColumn & column, const SerializationInfoSettings & settings) const override;
    using IDataType::getSerializationInfo;

    Float64 getDecayLength() const { return decay_length; }
    ExponentialTimeDecayingKeyWidth getKeyWidth() const { return key_width; }
    ExponentialTimeDecayingMode getMode() const { return mode; }
    const DataTypePtr & getNestedType() const { return storage_type; }
    const DataTypePtr & getDirectTupleType() const { return direct_type; }
    const DataTypePtr & getLogicalTupleType() const { return logical_type; }

private:
    const Float64 decay_length;
    const ExponentialTimeDecayingKeyWidth key_width;
    const ExponentialTimeDecayingMode mode;
    const DataTypePtr storage_type;
    const DataTypePtr direct_type;
    const DataTypePtr logical_type;
};

DataTypePtr createDataTypeExponentialTimeDecaying(
    Float64 decay_length,
    ExponentialTimeDecayingKeyWidth key_width = ExponentialTimeDecayingKeyWidth::Bits64,
    ExponentialTimeDecayingMode mode = ExponentialTimeDecayingMode::Compact);
std::optional<Float64> tryGetExponentialTimeDecayingDecayLength(const IDataType & type);
std::optional<Float64> tryGetExponentialTimeDecayingDecayLength(const DataTypePtr & type);
std::optional<ExponentialTimeDecayingKeyWidth> tryGetExponentialTimeDecayingKeyWidth(const IDataType & type);
std::optional<ExponentialTimeDecayingKeyWidth> tryGetExponentialTimeDecayingKeyWidth(const DataTypePtr & type);
std::optional<ExponentialTimeDecayingMode> tryGetExponentialTimeDecayingMode(const IDataType & type);
std::optional<ExponentialTimeDecayingMode> tryGetExponentialTimeDecayingMode(const DataTypePtr & type);
bool isExponentialTimeDecaying(const IDataType & type);
bool isExponentialTimeDecaying(const DataTypePtr & type);
bool containsExponentialTimeDecaying(const IDataType & type);
bool containsExponentialTimeDecaying(const DataTypePtr & type);

void assertExponentialTimeDecayingTypesCompatible(
    const DataTypePtr & left_type, const DataTypePtr & right_type, const String & operation);

void assertExponentialTimeDecayingConversionTypesCompatible(
    const DataTypePtr & source_type, const DataTypePtr & target_type, const String & operation);

void assertExponentialTimeDecayingSetKeyTypesCompatible(
    const DataTypePtr & probe_type, const DataTypePtr & set_type);

void validateExponentialTimeDecayingColumn(
    const IColumn & column, const String & operation);

ColumnPtr materializeExponentialTimeDecayingLogicalColumn(
    const IColumn & storage_column, Float64 decay_length);

ColumnPtr materializeExponentialTimeDecayingStorageColumn(
    const IColumn & logical_column,
    Float64 decay_length,
    const String & operation,
    ExponentialTimeDecayingKeyWidth key_width = ExponentialTimeDecayingKeyWidth::Bits64,
    ExponentialTimeDecayingMode mode = ExponentialTimeDecayingMode::Compact);

void validateExponentialTimeDecayingColumn(
    const IColumn & column, const DataTypePtr & type, const String & operation);

void registerDataTypeExponentialTimeDecaying(DataTypeFactory & factory);

}

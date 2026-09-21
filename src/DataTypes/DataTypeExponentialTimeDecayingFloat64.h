#pragma once

#include <DataTypes/IDataType.h>

#include <bit>
#include <cmath>
#include <optional>

namespace DB
{

class DataTypeFactory;

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

/// Compact 64-bit ordered representation of a decaying curve.
///
/// Zero occupies the midpoint. Negative curves occupy the range below it in
/// reverse unit-timestamp order and positive curves occupy the range above it
/// in forward unit-timestamp order. One bit of the sortable Float64 unit
/// timestamp is intentionally discarded to make room for the sign domain.
inline UInt64 shiftOneBitAndSign(
    Float64 unit_timestamp, Float64 value_at_anchor)
{
    constexpr UInt64 midpoint = UInt64(1) << 63;

    if (value_at_anchor == 0)
        return midpoint;

    const UInt64 sortable = getExponentialTimeDecayingSortableFloatKey(unit_timestamp);
    if (std::signbit(value_at_anchor))
        return midpoint - 1 - (sortable >> 1);

    return midpoint + (sortable >> 1) + (sortable & 1);
}

inline UInt64 getExponentialTimeDecayingOrderingKey(
    Float64 value, Float64 time, Float64 decay_length)
{
    return shiftOneBitAndSign(
        getExponentialTimeDecayingUnitTimestamp(value, time, decay_length),
        value);
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
        /// Each ordering key represents two neighboring sortable Float64 values.
        /// Pick the finite member of the pair at the infinities.
        if (negative)
            ++sortable;
        else
            --sortable;
        unit_timestamp = getExponentialTimeDecayingFloatFromSortableKey(sortable);
    }

    return {negative ? -1.0 : 1.0, unit_timestamp};
}

struct ExponentialTimeDecayingFloat64Value
{
    UInt64 ordering_key;
    Float64 value_at_anchor;
    Float64 anchor_time;
};

inline ExponentialTimeDecayingFloat64Value normalizeExponentialTimeDecayingFloat64(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (value == 0)
        return {shiftOneBitAndSign(0, 0), 0, 0};

    return {
        getExponentialTimeDecayingOrderingKey(value, time, decay_length),
        value,
        time};
}

inline bool isCanonicalExponentialTimeDecayingFloat64Value(
    UInt64 ordering_key, Float64 value, Float64 time, Float64 decay_length)
{
    if (!std::isfinite(value) || !std::isfinite(time))
        return false;

    const Float64 unit_timestamp
        = getExponentialTimeDecayingUnitTimestamp(value, time, decay_length);
    if (value != 0 && !std::isfinite(unit_timestamp))
        return false;

    const auto normalized = normalizeExponentialTimeDecayingFloat64(
        value, time, decay_length);
    return normalized.ordering_key == ordering_key
        && normalized.value_at_anchor == value
        && normalized.anchor_time == time;
}

class DataTypeExponentialTimeDecayingFloat64 final : public IDataType
{
public:
    explicit DataTypeExponentialTimeDecayingFloat64(Float64 decay_length_);

    TypeIndex getTypeId() const override { return TypeIndex::ExponentialTimeDecayingFloat64; }
    TypeIndex getColumnType() const override { return TypeIndex::Tuple; }
    String doGetName() const override;
    const char * getFamilyName() const override { return "ExponentialTimeDecaying"; }

    MutableColumnPtr createColumn() const override;
    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;
    bool isDefaultInsertTrivial() const override { return false; }

    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool canBeInsideNullable() const override { return true; }
    bool supportsSparseSerialization() const override { return true; }
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
    const DataTypePtr & getNestedType() const { return storage_type; }
    const DataTypePtr & getLogicalTupleType() const { return logical_type; }

private:
    const Float64 decay_length;
    const DataTypePtr storage_type;
    const DataTypePtr logical_type;
};

DataTypePtr createDataTypeExponentialTimeDecayingFloat64(Float64 decay_length);
std::optional<Float64> tryGetExponentialTimeDecayingFloat64DecayLength(const IDataType & type);
std::optional<Float64> tryGetExponentialTimeDecayingFloat64DecayLength(const DataTypePtr & type);
bool isExponentialTimeDecayingFloat64(const IDataType & type);
bool isExponentialTimeDecayingFloat64(const DataTypePtr & type);
bool containsExponentialTimeDecayingFloat64(const IDataType & type);
bool containsExponentialTimeDecayingFloat64(const DataTypePtr & type);

/// Rejects pairwise use when decaying values occupy different nested positions
/// or have different decay lengths.
void assertExponentialTimeDecayingFloat64TypesCompatible(
    const DataTypePtr & left_type, const DataTypePtr & right_type, const String & operation);

/// Permits a source whose exact semantic type is an alternative of a target Variant,
/// while otherwise requiring decaying values to keep their type identity.
void assertExponentialTimeDecayingFloat64ConversionTypesCompatible(
    const DataTypePtr & source_type, const DataTypePtr & target_type, const String & operation);

/// Set-key compatibility uses the same conversion rule.
void assertExponentialTimeDecayingFloat64SetKeyTypesCompatible(
    const DataTypePtr & probe_type, const DataTypePtr & set_type);

/// Rejects rows whose stored canonical ordering fields are invalid.
void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, const String & operation);

ColumnPtr materializeExponentialTimeDecayingFloat64LogicalColumn(
    const IColumn & storage_column, Float64 decay_length);

ColumnPtr materializeExponentialTimeDecayingFloat64StorageColumn(
    const IColumn & logical_column, Float64 decay_length, const String & operation);

/// Applies the same validation recursively when the experimental value is nested in
/// `Array`, `Tuple`, `Map`, `Variant`, `Nullable`, or `LowCardinality`.
void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, const DataTypePtr & type, const String & operation);

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory);

}

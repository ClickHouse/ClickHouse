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

/// Exact logical ordering identity for the Float64 ordered representation.
///
/// The middle value is zero. Negative curves occupy the range below it in
/// reverse unit-timestamp order, and positive curves occupy the range above it
/// in forward unit-timestamp order. A finite Float64 unit timestamp leaves the
/// whole key below 2^65, so the lossy compact representation is simply key >> 1.
inline UInt128 getExponentialTimeDecayingOrderingKeyFromUnitTimestamp(
    Float64 unit_timestamp, Float64 value_at_anchor)
{
    constexpr UInt128 midpoint = UInt128(1) << 64;

    if (value_at_anchor == 0)
        return midpoint;

    const UInt64 sortable = getExponentialTimeDecayingSortableFloatKey(unit_timestamp);
    if (std::signbit(value_at_anchor))
        return midpoint - UInt128(1) - UInt128(sortable);

    return midpoint + UInt128(1) + UInt128(sortable);
}

inline UInt128 getExponentialTimeDecayingOrderingKey(
    Float64 value, Float64 time, Float64 decay_length)
{
    return getExponentialTimeDecayingOrderingKeyFromUnitTimestamp(
        getExponentialTimeDecayingUnitTimestamp(value, time, decay_length),
        value);
}

/// Compact ordered projection. It intentionally discards the least-significant
/// bit of the exact 65-bit ordering identity.
inline UInt64 shiftOneBitAndSign(
    Float64 unit_timestamp, Float64 value_at_anchor)
{
    return static_cast<UInt64>(
        getExponentialTimeDecayingOrderingKeyFromUnitTimestamp(
            unit_timestamp, value_at_anchor)
        >> 1);
}

inline UInt64 getExponentialTimeDecayingOrderingPrefix(
    Float64 value, Float64 time, Float64 decay_length)
{
    return static_cast<UInt64>(
        getExponentialTimeDecayingOrderingKey(value, time, decay_length) >> 1);
}

struct ExponentialTimeDecayingCanonicalDirectValue
{
    Float64 value_at_anchor;
    Float64 anchor_time;
};

inline ExponentialTimeDecayingCanonicalDirectValue
getExponentialTimeDecayingCanonicalDirectValue(UInt128 ordering_key)
{
    constexpr UInt128 midpoint = UInt128(1) << 64;

    if (ordering_key == midpoint)
        return {0, 0};

    const bool negative = ordering_key < midpoint;
    const UInt64 sortable = negative
        ? static_cast<UInt64>(midpoint - UInt128(1) - ordering_key)
        : static_cast<UInt64>(ordering_key - midpoint - UInt128(1));
    const Float64 unit_timestamp = getExponentialTimeDecayingFloatFromSortableKey(sortable);
    return {negative ? -1.0 : 1.0, unit_timestamp};
}

struct ExponentialTimeDecayingFloat64Value
{
    UInt128 ordering_key;
    UInt64 ordering_prefix;
    Float64 value_at_anchor;
    Float64 anchor_time;
};

inline ExponentialTimeDecayingFloat64Value normalizeExponentialTimeDecayingFloat64(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (value == 0)
    {
        const auto ordering_key = getExponentialTimeDecayingOrderingKeyFromUnitTimestamp(0, 0);
        return {ordering_key, static_cast<UInt64>(ordering_key >> 1), 0, 0};
    }

    const Float64 unit_timestamp
        = getExponentialTimeDecayingUnitTimestamp(value, time, decay_length);
    const auto ordering_key
        = getExponentialTimeDecayingOrderingKeyFromUnitTimestamp(unit_timestamp, value);
    return {
        ordering_key,
        static_cast<UInt64>(ordering_key >> 1),
        value,
        time};
}

inline bool isCanonicalExponentialTimeDecayingFloat64Value(
    UInt64 ordering_prefix, Float64 value, Float64 time, Float64 decay_length)
{
    if (!std::isfinite(value) || !std::isfinite(time))
        return false;

    const Float64 unit_timestamp
        = getExponentialTimeDecayingUnitTimestamp(value, time, decay_length);
    if (value != 0 && !std::isfinite(unit_timestamp))
        return false;

    const auto normalized = normalizeExponentialTimeDecayingFloat64(
        value, time, decay_length);
    return normalized.ordering_prefix == ordering_prefix
        && normalized.value_at_anchor == value
        && normalized.anchor_time == time;
}

class DataTypeExponentialTimeDecayingFloat64 final : public IDataType
{
public:
    explicit DataTypeExponentialTimeDecayingFloat64(Float64 decay_length_, bool legacy_name_ = false);

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
    const bool legacy_name;
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

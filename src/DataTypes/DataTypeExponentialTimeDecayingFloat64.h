#pragma once

#include <DataTypes/IDataType.h>

#include <cmath>
#include <optional>

namespace DB
{

class DataTypeFactory;

struct ExponentialTimeDecayingFloat64Value
{
    Float64 sign;
    Float64 signed_unit_time;
};

/// This is also the native lexicographic sort key for a fixed decay length.
/// `sign` orders negative, zero, and positive curves. For positive curves the
/// value grows with unit_time; for negative curves it decreases, so storing
/// `-unit_time` gives the same ascending order as the curve value. The order is
/// therefore identical at every common evaluation time.
inline ExponentialTimeDecayingFloat64Value normalizeExponentialTimeDecayingFloat64(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (value == 0)
        return {0, 0};

    const Float64 sign = std::copysign(1.0, value);
    const Float64 unit_time = time + decay_length * std::log(std::abs(value));
    return {sign, sign * unit_time};
}

inline bool isCanonicalExponentialTimeDecayingFloat64Value(Float64 sign, Float64 signed_unit_time)
{
    if (sign == 0)
        return signed_unit_time == 0;

    return (sign == -1 || sign == 1) && std::isfinite(signed_unit_time);
}

inline Float64 getExponentialTimeDecayingUnitTime(Float64 sign, Float64 signed_unit_time)
{
    return sign * signed_unit_time;
}

class DataTypeExponentialTimeDecayingFloat64 final : public IDataType
{
public:
    explicit DataTypeExponentialTimeDecayingFloat64(Float64 decay_length_);

    TypeIndex getTypeId() const override { return TypeIndex::ExponentialTimeDecayingFloat64; }
    TypeIndex getColumnType() const override { return TypeIndex::Tuple; }
    String doGetName() const override;
    const char * getFamilyName() const override { return "ExponentialTimeDecayingFloat64"; }

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
    const DataTypePtr & getNestedType() const { return nested_type; }

private:
    const Float64 decay_length;
    const DataTypePtr nested_type;
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

/// Rejects rows whose redundant marker or canonical ordering fields do not match the type.
void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, Float64 decay_length, const String & operation);

/// Applies the same validation recursively when the experimental value is nested in
/// `Array`, `Tuple`, `Map`, `Variant`, `Nullable`, or `LowCardinality`.
void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, const DataTypePtr & type, const String & operation);

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory);

}

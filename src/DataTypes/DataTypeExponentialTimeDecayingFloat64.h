#pragma once

#include <DataTypes/DataTypeCustom.h>
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

class DataTypeCustomExponentialTimeDecayingFloat64 final : public IDataTypeCustomName
{
public:
    explicit DataTypeCustomExponentialTimeDecayingFloat64(Float64 decay_length_)
        : decay_length(decay_length_)
    {
    }

    String getName() const override;
    std::optional<Field> getDefault() const override;
    Float64 getDecayLength() const { return decay_length; }

private:
    const Float64 decay_length;
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

/// Rejects rows whose redundant marker or canonical ordering fields do not match the type.
/// Used before generic tuple comparison and sorting, which cannot see the custom type name.
void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, Float64 decay_length, const String & operation);

/// Applies the same validation recursively when the experimental value is nested in
/// Array, Tuple, Map, Nullable, or LowCardinality.
void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, const DataTypePtr & type, const String & operation);

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory);

}

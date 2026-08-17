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

inline ExponentialTimeDecayingFloat64Value normalizeExponentialTimeDecayingFloat64(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (value == 0)
        return {0, 0};

    const Float64 sign = std::copysign(1.0, value);
    const Float64 unit_time = time + decay_length * std::log(std::abs(value));
    return {sign, sign * unit_time};
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

/// Rejects rows whose redundant marker or canonical ordering fields do not match the type.
/// Used before generic tuple comparison and sorting, which cannot see the custom type name.
void validateExponentialTimeDecayingFloat64Column(
    const IColumn & column, Float64 decay_length, const String & operation);

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory);

}

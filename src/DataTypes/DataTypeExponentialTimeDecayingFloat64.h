#pragma once

#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/IDataType.h>

#include <optional>

namespace DB
{

class DataTypeFactory;

class DataTypeCustomExponentialTimeDecayingFloat64 final : public IDataTypeCustomName
{
public:
    explicit DataTypeCustomExponentialTimeDecayingFloat64(Float64 decay_length_)
        : decay_length(decay_length_)
    {
    }

    String getName() const override;
    Float64 getDecayLength() const { return decay_length; }

private:
    const Float64 decay_length;
};

DataTypePtr createDataTypeExponentialTimeDecayingFloat64(Float64 decay_length);
std::optional<Float64> tryGetExponentialTimeDecayingFloat64DecayLength(const DataTypePtr & type);
bool isExponentialTimeDecayingFloat64(const DataTypePtr & type);

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory);

}

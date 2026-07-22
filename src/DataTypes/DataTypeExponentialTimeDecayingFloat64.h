#pragma once

#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/IDataType.h>

#include <utility>

namespace DB
{

class DataTypeFactory;

class DataTypeExponentialTimeDecayingFloat64Name final : public IDataTypeCustomName
{
public:
    explicit DataTypeExponentialTimeDecayingFloat64Name(DataTypePtr time_type_)
        : time_type(std::move(time_type_))
    {
    }

    String getName() const override;
    const DataTypePtr & getTimeType() const { return time_type; }

private:
    DataTypePtr time_type;
};

DataTypePtr createDataTypeExponentialTimeDecayingFloat64(const DataTypePtr & time_type);
bool isExponentialTimeDecayingFloat64(const DataTypePtr & type);
const DataTypePtr & getExponentialTimeDecayingFloat64TimeType(const DataTypePtr & type);

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory);

}

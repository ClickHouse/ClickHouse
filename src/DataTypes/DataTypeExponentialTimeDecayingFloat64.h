#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

class DataTypeFactory;

DataTypePtr createDataTypeExponentialTimeDecayingFloat64();
bool isExponentialTimeDecayingFloat64(const DataTypePtr & type);

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory);

}

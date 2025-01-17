#pragma once
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

Field parseFieldFromString(const String & value, DB::DataTypePtr data_type);

}

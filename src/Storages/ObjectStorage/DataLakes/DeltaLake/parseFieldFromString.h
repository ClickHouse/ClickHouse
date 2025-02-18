#pragma once
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// With known `data_type` of `value`, parse `value` into DB::Field.
Field parseFieldFromString(const String & value, DB::DataTypePtr data_type);

}

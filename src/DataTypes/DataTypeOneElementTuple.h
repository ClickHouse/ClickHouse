#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

DataTypePtr createOneElementTuple(const DataTypePtr & type, const String & name, bool escape_delimiter = true);

}

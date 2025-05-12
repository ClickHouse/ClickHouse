#pragma once

#include <base/types.h>
#include <Core/MultiEnum.h>
#include <DataTypes/IDataType.h>
namespace DB
{
DataTypePtr convertYTsaurusDataType(const String & data_type, bool required);
}

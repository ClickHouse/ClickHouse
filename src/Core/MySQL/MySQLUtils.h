#pragma once

#include <Core/DecimalFunctions.h>
#include <DataTypes/IDataType.h>

namespace DB
{
namespace MySQLProtocol
{
namespace MySQLUtils
{
/// Splits DateTime64 column data at a certain row number into whole and fractional part
/// Additionally, normalizes the fractional part as if it was scale 6 for MySQL compatibility purposes
DecimalUtils::DecimalComponents<DateTime64> getNormalizedDateTime64Components(DataTypePtr data_type, ColumnPtr col, size_t row_num);
}
}
}

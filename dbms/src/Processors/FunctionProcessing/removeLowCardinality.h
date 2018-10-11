#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

DataTypePtr recursiveRemoveLowCardinality(const DataTypePtr & type);
ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column);

}

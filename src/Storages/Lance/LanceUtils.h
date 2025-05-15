#pragma once

#include "config.h"

#if USE_LANCE

#    include <lance.h>

#    include <DataTypes/IDataType.h>

namespace DB
{

DataTypePtr lanceColumnTypeToDataType(lance::ColumnType type);
lance::ColumnType typeIndexToLanceColumnType(TypeIndex type);
lance::ColumnType dataTypePtrToLanceColumnType(DataTypePtr type);

} // namespace DB

#endif

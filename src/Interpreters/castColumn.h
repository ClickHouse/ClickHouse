#pragma once

#include <Core/ColumnWithTypeAndName.h>

namespace DB
{

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type);
ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type);
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type);

}
